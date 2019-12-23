package com.jentekllc.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.util.LongAccumulator
import org.apache.log4j._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql._

object ConvertCSV2Parquet {
  
def main(args: Array[String]) {  
val spark = SparkSession
    .builder
    .appName("csv2parquet")
    .master("local[*]")
    .getOrCreate()
    

val rdd = spark.read.format("csv").option("header", "true").load("e:/tmp/bank_customer.csv")
val df: DataFrame = rdd.toDF()


df.show(false)

val df_with_datatype=df.selectExpr("name_string",
                  "cast(age_int as int) age_int", 
                  "cast(balance_float as float) balance_float",
                  "cast(checking_bool as boolean) checking_bool")

df_with_datatype.show(false)

df_with_datatype.write.parquet("e:/tmp/bank_customer.parquet")

val read_parquet_df = spark.read.parquet("e:/tmp/bank_customer.parquet")

read_parquet_df.show(false)

}
}