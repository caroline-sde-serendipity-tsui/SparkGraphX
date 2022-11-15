import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.rdd.RDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.graphx.*;
import org.apache.spark.graphx.lib.ConnectedComponents;
import org.apache.spark.graphx.lib.StronglyConnectedComponents;

import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.lang.*;

public class SCC implements Serializable {
    private static JavaSparkContext jsc;

    // Must explicitly provide for implicit Scala parameters in various function calls.
    private static final ClassTag<Integer> tagInteger = ClassTag$.MODULE$.apply(Integer.class);
    private static final ClassTag<Integer> tagLong    = ClassTag$.MODULE$.apply(Long.class);
    private static final ClassTag<String> tagString   = ClassTag$.MODULE$.apply(String.class);
    private static final ClassTag<Object> tagObject   = ClassTag$.MODULE$.apply(Object.class);
    
    public static void startSpark() {
        // Set conf and context
        jsc = new JavaSparkContext(new SparkConf().setAppName("Strongly Connected Components").setMaster("local"));
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

    public static List<Tuple2<Long,Iterable<Long>>> scc(JavaRDD<Edge<Integer>> edgesRDD) {
        Graph<String, Integer> graphRDD = Graph.fromEdges(edgesRDD.rdd(), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), tagString, tagInteger);
        
        Graph<Object, Integer> sccRDD = StronglyConnectedComponents.run(graphRDD, (int) edgesRDD.count(), tagString, tagInteger);

        List<Tuple2<Long,Iterable<Long>>> sccResult = sccRDD.vertices().toJavaRDD().mapToPair(new PairFunction<Tuple2<Object, Object>, Long, Long>(){
            @Override
            public Tuple2<Long, Long> call(Tuple2<Object, Object> vertex) throws Exception {
                return new Tuple2((Long)vertex._2, (Long)vertex._1);
            }
        }).groupByKey().collect();

        return sccResult;
    }

    public static void stopSpark() {
        // Stop the Spark application.
        jsc.stop();
    }

    static class AbsFunc1 extends AbstractFunction1<Edge<Integer>, Boolean> implements Serializable {
        @Override
        public Boolean apply(Edge<Integer> e) {
            return false;
        }
    }

    static class WeightComparator implements Comparator<Edge<Integer>>, Serializable {
        @Override
        public int compare(Edge e1, Edge e2) {
            return (int)e1.attr() - (int)e2.attr();
        }
    }

    public static void main(String[] args) {
        startSpark();
        // String filePath = "InputGraphs/DirectedGraphs/" + args[0] + ".txt";
        String filePath = "InputGraphs/DirectedGraphs/" + "datasettest" + ".txt";

        // Start timer
        long startTime = System.currentTimeMillis();

        JavaRDD<Edge<Integer>> edgesRDD = generateGraph(filePath);
        List<Tuple2<Long,Iterable<Long>>> sccResult = scc(edgesRDD);
        stopSpark();
        
        // Stop timer
        long endTime = System.currentTimeMillis();
        System.out.println("\n[Found Strongly Connected Components] " + sccResult);
        System.out.println("[Number of Components] " + sccResult.size());
        System.err.println("[Elapsed Time] " + (endTime - startTime) + " ms\n");
    }
}