package com.jentekco.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._

import org.apache.log4j.{Level, Logger}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/** Listens to a stream of Tweets only contains "happy" or "money" case insensitive
you can change it to whatever keyword you want to limit to

George Jen
Jen Tek LLC


 */
object SearchKeywordTweets {
  
 /** 
I have included working build.sbt for you to run 
sbt assembly to create jar file, check sbt sub folder for the build.sbt
*/
 
  def main(args: Array[String]) {
    
    var onetime=true
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Configure Twitter credentials using twitter.txt
//    setupTwitterConfig()
    
    val sdf = new SimpleDateFormat("YYYY-MM-DD hh:mm:ss");
    val sparkConf = new SparkConf().setAppName("getTweets").setMaster("local[3]")
    // Create the context
    
      val spark = SparkSession
          .builder()
          .config("spark.master", "local[2]")
          .appName("interfacing spark sql to hive metastore through thrift url below")
          .config("hive.metastore.uris", "thrift://10.0.0.46:9083") // replace with your hivemetastore service's thrift url
          .enableHiveSupport() // to enable hive support
          .getOrCreate()
     import spark.implicits._
     spark.sql("CREATE TABLE IF NOT EXISTS tweets (datetime STRING, text STRING) USING hive")

     val sc=spark.sparkContext

     val ssc = new StreamingContext(sc, Seconds(1))
    
 // specify twitter consumerKey, consumerSecret, accessToken, accessTokenSecret   
 // These keys below are NOT valid, only for demo purpose.  Do not waste time to run with these keys
    System.setProperty("twitter4j.oauth.consumerKey","dgHDhsGEpNAY5tTTe2Oi0Aw1V")
    System.setProperty("twitter4j.oauth.consumerSecret", "tx4G4iFZ2z8nLgiOMwqT6K9PTkE75qI0da6SGeF5o5TAJ2VuAU")
    System.setProperty("twitter4j.oauth.accessToken", "3129381637-I59MeAXCj7iHfaOb7AnHAwh5s9B5YNfj2S0pYFb")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "oJdQ7tQMDmGlpjpp5QI0ZrR9OU58eFqeyev1QOpMBeypk")
    
// Connect to Twitter and get the tweets object     
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // Extract the text from the tweets object
    val tweets_collection = tweets.map(each_tweet => each_tweet.getText())
    
    //Set your search criteria to only retain these meet your search condition
    val focus_tweets_collection=tweets_collection.filter(text=>text.toLowerCase.contains("happy") | text.toLowerCase.contains("money"))
      
    //iterate through DSTREAM for each RDD, show it and store tweets streaming 
    // to HIVE table, this is an example of as part of ETL to harvest and store
    // streaming data from Twitter
    focus_tweets_collection.foreachRDD{ rdd =>
         if(!rdd.isEmpty) {
             val tweetsDataFrame = rdd.toDF("newTweet")
             tweetsDataFrame.show(false)
//             tweetsDataFrame.printSchema()
//             println(tweetsDataFrame.count())
//Create in memory temp view newTweets
             tweetsDataFrame.createOrReplaceTempView("newTweets")
// Inserts the new tweets received to HIVE table tweets, with 
// current datetimestamp
             spark.sql("insert into tweets select from_unixtime(unix_timestamp()), * from newTweets")

        }
      }
//If you intent to run it on non windows machine, change below to proper path
    ssc.checkpoint("d:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}
