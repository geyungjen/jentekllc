package com.jentekco.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._

import org.apache.log4j.{Level, Logger}

/** Listens to a stream of Tweets only contains "new york" or "san francisco" case insensitive
you can change it to whatever keyword you want to limit to
 */
object SearchKeywordTweets {
  
  /** 
You will need to include external jar files for you to be able to import org.apache.spark.streaming.twitter._
they are:

dstream-twitter_<your version of Scala>-SNAPSHOT.jar
twitter4j-core-4.0.4.jar
twitter4j-stream-4.0.4.jar

They are downloadable online, you need to search and download

*/
 
def main(args: Array[String]) {
    
    
Logger.getLogger("org").setLevel(Level.ERROR)    
val ssc = new StreamingContext("local[*]", "SearchTwitterTweets", Seconds(1))
    System.setProperty("twitter4j.oauth.consumerKey","<your Twitter consumer key>")
System.setProperty("twitter4j.oauth.consumerSecret", "<your twitter consumer secret>")
System.setProperty("twitter4j.oauth.accessToken", "<your twitter access token>")
System.setProperty("twitter4j.oauth.accessTokenSecret", "<your twitter token secret>")
    
    
val tweets = TwitterUtils.createStream(ssc, None)
    
// Extract the text
val tweets_collection = tweets.map(each_tweet => each_tweet.getText())
    
//Set your search criteria to only retain these meet your search condition
val focus_tweets_collection=tweets_collection.filter(text=>text.toLowerCase.contains("new york") | text.toLowerCase.contains("san francisco"))

//Display your result
focus_tweets_collection.print()
    
ssc.checkpoint("d:/checkpoint/")
ssc.start()
ssc.awaitTermination()
  }  
}