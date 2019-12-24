package com.jentekco.spark


import java.io.File
import org.apache.log4j._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}


object SparkHive {

  
  case class Record(key: Int, value: String)
 

  def main(args: Array[String]): Unit = {
 
  Logger.getLogger("org").setLevel(Level.ERROR)
  val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  val spark = SparkSession
          .builder()
          .config("spark.master", "local")
          .appName("interfacing spark sql to hive metastore with no configuration file")
          .config("hive.metastore.uris", "thrift://10.0.0.46:9083") // replace with your hivemetastore service's thrift url
          .enableHiveSupport() // to enable hive support
          .getOrCreate()


    import spark.implicits._
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    sql("LOAD DATA LOCAL INPATH 'D:/spark/examples/src/main/resources/kv1.txt' INTO TABLE src")    
    sql("SELECT * FROM src").show()
    sql("SELECT COUNT(*) FROM src").show()
  
    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    stringsDS.show()

    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")

    sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()
    sql("CREATE TABLE IF NOT EXISTS hive_records(key int, value string) STORED AS PARQUET")

    val df = spark.table("src")
    df.write.mode(SaveMode.Overwrite).saveAsTable("hive_records")

    sql("SELECT * FROM hive_records").show()

    val dataDir = "/tmp/parquet_data"
    spark.range(10).write.parquet(dataDir)

    sql(s"CREATE EXTERNAL TABLE IF NOT EXISTS hive_bigints(id bigint) STORED AS PARQUET LOCATION '$dataDir'")

    sql("SELECT * FROM hive_bigints").show()

    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    df.write.partitionBy("key").format("hive").saveAsTable("hive_part_tbl")
 
    sql("SELECT * FROM hive_part_tbl").show()


    spark.stop()
 
  }
}
