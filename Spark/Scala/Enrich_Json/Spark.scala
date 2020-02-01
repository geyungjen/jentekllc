package com.jentekco.enrichJsonNew

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.util.LongAccumulator
import org.apache.log4j._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j._


object Spark {
        def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.ERROR)
      val rec1: String = """{
    "visitorId": "v1",
    "products": [{
         "id": "i1",
         "interest": 0.68
    }, {
         "id": "i2",
         "interest": 0.42
    }]
}"""
      
      val rec2: String = """{
    "visitorId": "v2",
    "products": [{
         "id": "i1",
         "interest": 0.78
    }, {
         "id": "i3",
         "interest": 0.11
    }]
}"""
      
      val visitsData: Seq[String] = Seq(rec1, rec2)
      val productIdToNameMap = Map("i1" -> "Nike Shoes", "i2" -> "Umbrella", "i3" -> "Jeans")
      
      //Solution starts here
      val spark = SparkSession
    .builder
    .appName("JsonApp")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///d:/tmp")
    .getOrCreate()
      
      import spark.implicits._
      import spark.sql
      
      productIdToNameMap.toSeq.toDF("id","name").createOrReplaceTempView("prodRec")
      for (i<-visitsData)
    {
//      println(rec)
    println("Original Json String is: \n")
    println(i)
    println("\n")
    var rec=spark.read.json(Seq(i).toDS) 
    rec.createOrReplaceTempView("dfVisitsTable")
//    sql("select * from dfVisitsTable").show()
    val productsArr=sql ("SELECT products FROM dfVisitsTable").withColumn("products", explode($"products")).select("products.*")
//    productsArr.show(false)
    productsArr.createOrReplaceTempView("productsArr")
//    val enrichedProducts=sql("select a.id, b.name, a.interest from productsArr a, prodRec b where a.id=b.id")
//    enrichedProducts.show(false)
    //  Need to do outer join in case the product id in the record is not valid, if product id not found in the MAP,
//  return invalid product
    val enrichedProducts=sql("select a.id, if (b.name is not null, b.name, 'invalid product') name, a.interest from productsArr a full outer join prodRec b on a.id=b.id")     
    val enrichedRecord=rec.select("VisitorId").join(enrichedProducts)
//    enrichedRecord.show(false)
    enrichedRecord.createOrReplaceTempView("enrichedRec")
//    sql("select visitorId, collect_list(struct(id, name, interest)) products from enrichedRec group by visitorId").show(false)
    val enrichedJson=sql("select visitorId, collect_list(struct(id, name, interest)) products from enrichedRec group by visitorId").toJSON
    .collect.mkString("",",","")
    println("Enriched Json String is:\n")
    println(enrichedJson)
    println(" ")
    println(" ")
    }
     } 

 
}