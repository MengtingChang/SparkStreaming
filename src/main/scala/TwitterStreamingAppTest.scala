import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._

import java.util.Date
import org.apache.log4j.{Level, Logger}


object TwitterStreamingAppTest {
  def main(args: Array[String]) {

    if (args.length < 4) {
      System.err.println("Usage: TwitterStreamingApp <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
    // to run this application inside an IDE, comment out previous line and uncomment line below
    val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[*]")

    val sc = new SparkContext(sparkConf);
    sc.setCheckpointDir("/tmp");
    val ssc = new StreamingContext(sc, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    val stopWordsRDD = ssc.sparkContext.textFile("src/main/resources/stop-words.txt")
    val posWordsRDD = ssc.sparkContext.textFile("src/main/resources/pos-words.txt")
    val negWordsRDD = ssc.sparkContext.textFile("src/main/resources/neg-words.txt")

    val positiveWords = posWordsRDD.collect().toSet
    val negativeWords = negWordsRDD.collect().toSet
    val stopWords = stopWordsRDD.collect().toSet

    val englishTweets = stream.filter(status => status.getUser().getLang() == "en")
    
    val tweets = englishTweets.map(status => status.getText())
    
    // Numbers of tweets (for debugging)
    //val numberOfTweets = tweets.map(t => 1).reduce(_+_)
    //numberOfTweets.print()

    // save raw tweets data (for debugging)
    // tweets.saveAsTextFiles()
    
    // UDF for spliting a piece of text into individual words.
    def tokenize(text : String) : Array[String] = {
      // Lowercase each word and remove punctuation/stop words/user ID.
      text.toLowerCase.replaceAll("[^a-zA-Z0-9@\\s]", "").split("\\s+").filter(!stopWords.contains(_)).filter(!_.startsWith("@"))
    }
     
    // For each tweet, break the message into tokens, then remove punctuation marks and stop words
    val wordsList = tweets.map(word => tokenize(word))

    // UDF for scoring each tweet
    def scoreTweet (splitedTweet: Array[String]) : Int = {
      splitedTweet.map(word => if(positiveWords.contains(word)) 1
      else if (negativeWords.contains(word)) -1
      else 0
      ).sum
    }
  
    
    //print score of each tweet
    val tweetScore = wordsList.map(tweet => scoreTweet(tweet))
    tweetScore.print()

    
    // Determine whether a tweet has a positive, negative or neutral sentiment    
    val sentiment = wordsList.map(tweet =>{
      if (scoreTweet(tweet) > 0) 
      {
        ("positive", 1)
      }
      else if(scoreTweet(tweet) == 0)
      {
        ("neutral", 1)
      }
      else
      {
        ("negative", 1)
      }
    }).reduceByKey(_ + _)
    
    // print sentiment(for debugging)
    // sentiment.print()
    
    
    // Comment out line below to reduce last 30 seconds of data, displays every 10 seconds
    //val windowedWordCounts = sentiment.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(30), Seconds(10))
    // Comment out line below to reduce last 10 seconds of data, displays every 10 seconds
    val windowedWordCounts = sentiment.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(10), Seconds(4))
    
    // Comment out line below to incrementally reduce last 30 seconds of data, displays every 10 seconds
    // val windowedWordCounts = sentiment.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), (a:Int,b:Int) => (a - b), Seconds(30), Seconds(10))
    // Or comment out incrementally reduce last 10 seconds of data, displays every 4 seconds
    // val windowedWordCounts = sentiment.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), (a:Int,b:Int) => (a - b), Seconds(10), Seconds(4))

    windowedWordCounts.foreachRDD(rdd=>
      rdd.collect().foreach(f => println(s"Counts of past window: $f")))
    
    // function for accumulating counts
    val updateFunc = (newValues: Seq[Int], state: Option[Int]) => {
      val currentCount = newValues.sum
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
      }
    
    // accumulating counts
    val stateDstream = sentiment.updateStateByKey[Int](updateFunc)
    
    stateDstream.foreachRDD(rdd => 
      rdd.collect().foreach(f => println(s"Stateful(Total) counts: $f")))
    //stateDstream.print()
    
    // print tweets in the currect DStream 
    tweets.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
}