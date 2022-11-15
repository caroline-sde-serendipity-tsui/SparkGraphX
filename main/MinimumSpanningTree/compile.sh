#!/bin/sh

/usr/lib/jvm/java-1.8.0/bin/javac -cp spark-2.3.1-bin-hadoop2.7/jars/spark-core_2.11-2.3.1.jar:spark-2.3.1-bin-hadoop2.7/jars/spark-sql_2.11-2.3.1.jar:spark-2.3.1-bin-hadoop2.7/jars/scala-library-2.11.8.jar:spark-2.3.1-bin-hadoop2.7/jars/spark-graphx_2.11-2.3.1.jar:google-collections-1.0.jar $1.java
/usr/lib/jvm/java-1.8.0/bin/jar -cvf $1.jar $1*.class
