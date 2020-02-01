package com.jentekco.spark

import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.log4j._
import org.apache.spark.sql._

import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.graphx.util.GraphGenerators


object orgchart {
 def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
    .builder
    .appName("graphx")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///tmp")
    .getOrCreate()
    
    val sc=spark.sparkContext
    
    val users: RDD[(VertexId, (String, String))] =
        sc.parallelize(Array((1L, ("jack", "owner")), (2L, ("george", "clerk")),
                       (3L, ("mary", "sales")), (4L, ("sherry", "owner wife"))))
    val relationships: RDD[Edge[String]] =
        sc.parallelize(Array(Edge(1L, 2L, "boss"),    Edge(1L, 3L, "boss"),
                       Edge(2L, 3L, "coworker"), Edge(4L, 1L, "boss")))
    val defaultUser = ("", "Missing")
    
    val graph = Graph(users, relationships, defaultUser)
    /*
The EdgeTriplet class extends the Edge class by adding the srcAttr and dstAttr members 
which contain the source and destination properties respectively. 
We can use the triplet view of a graph to render a collection of strings describing relationships between users.
*/

   val facts: RDD[String] =
       graph.triplets.map(triplet =>
       triplet.srcAttr._1 + ", "+ triplet.srcAttr._2 + " is the " + triplet.attr + " of " + triplet.dstAttr._1+", "+triplet.dstAttr._2)

   facts.collect.foreach(println(_))   
    
 }
}