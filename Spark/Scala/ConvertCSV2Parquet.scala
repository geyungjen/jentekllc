package com.jentekco.spark
//George Jen, Jen Tek LLC
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.util.LongAccumulator
import org.apache.log4j._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql._

object ConvertCSV2Parquet {
  
def main(args: Array[String]) {
Logger.getLogger("org").setLevel(Level.ERROR)
val spark = SparkSession
    .builder
    .appName("csv2parquet")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///d:/tmp")
    .getOrCreate()
    

val ds = spark.read.format("csv").option("header", "true").option("quote", "\"").load("D:/teaching/scala/ticker_symbol.csv")
val df: DataFrame = ds.toDF()


df.show(3, false)

//When the CSV file was read into DataFrame, all fields are String, below is to cast it to
//what the data should be, such as cast CategoryNumber to Int

val df_with_datatype=df.selectExpr("Ticker",
                  "Name", 
                  "Exchange",
                  "CategoryName",
                  "cast(CategoryNumber as int) CategoryNumber")

df_with_datatype.show(3, false)

//Save the DataFrame to Parquet format, overwrite if existing.
//Parquet is Columnar, good for Analytics query.

df_with_datatype.write.mode(SaveMode.Overwrite).parquet("D:/teaching/scala/ticker_symbol.parquet")

//Read the Parquet data back and run SQL query on it

val read_parquet_df = spark.read.parquet("D:/teaching/scala/ticker_symbol.parquet")

read_parquet_df.show(3, false)

import spark.implicits._
    val TickerSymbol = read_parquet_df.toDF()
    
    TickerSymbol.printSchema()
    
    TickerSymbol.createOrReplaceTempView("TickerSymbol")
    
    spark.sql("SELECT * from TickerSymbol where Ticker in ('IBM','MSFT','HPQ','GE')").show(20,false)

}
}