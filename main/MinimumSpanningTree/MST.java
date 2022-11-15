import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.rdd.RDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.graphx.*;
import org.apache.spark.graphx.lib.ConnectedComponents;

import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.lang.*;

public class MST implements Serializable {
    private static JavaSparkContext jsc;

    // Must explicitly provide for implicit Scala parameters in various function calls.
    private static final ClassTag<Integer> tagInteger = ClassTag$.MODULE$.apply(Integer.class);
    private static final ClassTag<Integer> tagLong    = ClassTag$.MODULE$.apply(Long.class);
    private static final ClassTag<String> tagString   = ClassTag$.MODULE$.apply(String.class);
    private static final ClassTag<Object> tagObject   = ClassTag$.MODULE$.apply(Object.class);
    
    public static void startSpark() {
        // Set conf and context
        jsc = new JavaSparkContext(new SparkConf().setAppName("Minimum Spanning Tree").setMaster("local"));
    }

    public static JavaRDD<Edge<Integer>> generateGraph(String filePath) {
        // Create a RDD from file
        JavaRDD<String> textFileRDD = jsc.textFile(filePath);

        // Read each line from RDD and create edges RDD
        JavaRDD<Edge<Integer>> edgesRDD = textFileRDD.map(line -> {
            String[] attrs = line.split(",");
            Edge<Integer> edge = new Edge<>(Long.parseLong(attrs[0]), Long.parseLong(attrs[1]), Integer.parseInt(attrs[2]));
            return edge;
        });
        return edgesRDD;
    }

    public static int mst(JavaRDD<Edge<Integer>> edgesRDD) {
        Graph<String, Integer> graphRDD = Graph.fromEdges(edgesRDD.rdd(), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), tagString, tagInteger);
        int sumOfWeights = 0;

        // Initilized an empty RDD for storing MST result.
        VertexRDD<String> mstVerticesRDD = graphRDD.vertices();
        // graphRDD.unpersist();
        JavaRDD<Edge<Integer>> mstEdgesRDD = jsc.parallelize(new ArrayList<>());
        Graph<String, Integer> mstGraphRDD;
        Graph<Object, Integer> ccRDD;

        long numOfVertices = mstVerticesRDD.count();
        System.out.println("[#mstVerticesRDD] " + mstVerticesRDD.count() + "\n");
        
        // GraphOps<String, Integer> mstGraphOpsRDD = graphRDD.graphToGraphOps(graphRDD, tagString, tagInteger);
        // int numOfEdges = (int) mstGraphOpsRDD.numEdges();
        // System.out.println("[numOfEdges] " + numOfEdges);

        // If the two vertices of the newly discovered edge are not in the same component.
        for (int i = 0; mstEdgesRDD.count() < numOfVertices && 0 < edgesRDD.count(); i++) {
            Comparator<Edge<Integer>> myComparator = new WeightComparator();

            // Find the minimum weight edge in current iteration.            
            Edge<Integer> minEdgeCur = edgesRDD.min(myComparator);
            // print out the current minimum
            System.out.println("[iteration-" + i + "] minEdgeCur: " + minEdgeCur.srcId() + "," + minEdgeCur.dstId() + "," + minEdgeCur.attr());

            // Update current MST graph
            mstGraphRDD = Graph.apply(mstVerticesRDD.toJavaRDD().rdd(), mstEdgesRDD.rdd(), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), tagString, tagInteger);

            // Get the latest Connected Components
            ccRDD = ConnectedComponents.run(mstGraphRDD, tagString, tagInteger);

            // Check if the two vertices of the current minimum weight edge are not on the same component.
            Long srcVertexCid = (Long) ccRDD.vertices().toJavaRDD().filter(
                new Function<Tuple2<Object, Object>, Boolean>(){
                    public Boolean call(Tuple2<Object, Object> v) {
                        return ((Long)v._1).equals(minEdgeCur.srcId());
                    }
                }).collect().get(0)._2;

            Long destVertexCid = (Long) ccRDD.vertices().toJavaRDD().filter(
                new Function<Tuple2<Object, Object>, Boolean>(){
                    public Boolean call(Tuple2<Object, Object> v) {
                        return ((Long)v._1).equals(minEdgeCur.dstId());
                    }
                }).collect().get(0)._2;

            // Create the new EdgeRDD
            JavaRDD<Edge<Integer>> newMinEdgeRDD = jsc.parallelize(Arrays.asList(minEdgeCur));

            // If the two vertices are not on the same component, add the edge to the MST.
            if (!srcVertexCid.equals(destVertexCid)) {
                // Add edge.
                mstEdgesRDD = mstEdgesRDD.union(newMinEdgeRDD);
                System.out.println("[iteration-" + i + "] mstEdgeRDD: " + mstEdgesRDD.collect());

                // Accumulate weights.
                sumOfWeights += minEdgeCur.attr();
                System.out.println("[iteration-" + i + "] sumOfWeights: " + sumOfWeights);
            }

            // Remove the edge from the graph.
            newMinEdgeRDD = jsc.parallelize(Arrays.asList(minEdgeCur, 
                                                          new Edge<Integer>(minEdgeCur.dstId(),
                                                                            minEdgeCur.srcId(),
                                                                            minEdgeCur.attr())));
            edgesRDD = edgesRDD.subtract(newMinEdgeRDD);
            System.out.println("[iteration-" + i + "] #mstEdgesRDD: " + mstEdgesRDD.count());
            System.out.println("[iteration-" + i + "] #edgesRDD: " + edgesRDD.count());
            System.out.println();

            if (mstEdgesRDD.count() == numOfVertices - 1) {
                break;
            }
        }

        
        System.out.println("[Minimum Spanning Tree]");
        System.out.println("Edge(source vertex id, dest vertex id, edge weight)");
        mstEdgesRDD.collect().forEach(e ->
            System.out.println(e)
        );
        return sumOfWeights;
    }

    public static void stopSpark() {
        // Stop the Spark application.
        jsc.stop();
    }

    static class WeightComparator implements Comparator<Edge<Integer>>, Serializable {
        @Override
        public int compare(Edge e1, Edge e2) {
            return (int)e1.attr() - (int)e2.attr();
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("\n[WARN] Missing dataset argument \n- You probably forgot to type in a dataset, please try again. Thanks!");
            System.out.println("[Expected input] $ ./run.sh MST <name_of_dataset>");
            System.out.println("[For example]    $ ./run.sh MST datasettest\n");
            System.exit(-1);
        }

        startSpark();
        String filePath = "InputGraphs/UndirectedGraphs/" + args[0] + ".txt";
        
        // Start timer
        long startTime = System.currentTimeMillis();

        JavaRDD<Edge<Integer>> edgesRDD = generateGraph(filePath);
        int sumOfWeights = mst(edgesRDD);
        stopSpark();
        
        // Stop timer
        long endTime = System.currentTimeMillis();
        System.out.println("[Sum of weights] " + sumOfWeights);
		System.err.println("\n[Elapsed Time] " + (endTime - startTime) + " ms\n");
    }
}