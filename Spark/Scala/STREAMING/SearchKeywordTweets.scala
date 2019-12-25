package com.jentekco.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._

import org.apache.log4j.{Level, Logger}

/** Listens to a stream of Tweets only contains "santa" and "merry" case insensitive
you can change it to whatever keyword you want to limit to
 */
object SearchKeywordTweets {
  
  /** Configures Twitter consumer/access token credential using config.txt in the main workspace directory, the file looks like following (be careful not to add extra
space at each end of line):

consumerKey <copy and paste your consumer key>
consumerSecret <copy and paste your consumer secret>
accessToken <copy and paste your access token>
accessTokenSecret <copy and paste your token secret>
<extra one blank line here>

You will need to include external jar files for you to be able to import org.apache.spark.streaming.twitter._
they are:

dstream-twitter_<your version of Scala>-SNAPSHOT.jar
twitter4j-core-4.0.4.jar
twitter4j-stream-4.0.4.jar

They are downloadable online, you need to search and download

*/
  def setupTwitterConfig() = {
    import scala.io.Source
    
    for (line <- Source.fromFile("D:/eclipse_workspace/spark/src/com/jentekco/spark/config.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }
  
  def main(args: Array[String]) {
    
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Configure Twitter credentials using twitter.txt
    setupTwitterConfig()
    
    val ssc = new StreamingContext("local[*]", "SearchTwitterTweets", Seconds(1))
    
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // Extract the text
    val tweets_collection = tweets.map(each_tweet => each_tweet.getText())
    
    //Set your search criteria to only retain these meet your search condition
    val focus_tweets_collection=tweets_collection.filter(text=>text.toLowerCase.contains("santa") | text.toLowerCase.contains("merry"))

    //Display your result
    focus_tweets_collection.print()
    
    ssc.checkpoint("d:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}
