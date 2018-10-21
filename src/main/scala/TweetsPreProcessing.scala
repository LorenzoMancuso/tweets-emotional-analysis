import java.io.File

import LexicalResPreProcessingAlt.CleanLRName

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.lit

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object TweetsPreProcessing{

  def PreProcessing(sc:SparkContext): Unit ={
    val path:String="./DATASET/Tweets/"
    val sqlContext:SparkSession = SparkSession
      .builder()
      .appName("MAADB - progetto")
      .master("local[*]")
      .getOrCreate()

    var tweets:DataFrame=null

    GetFileList(path).foreach{fileName=>
      if(tweets==null){
        tweets=ReadFile(path,fileName,fileName.split("_")(2),sc,sqlContext)
      }else{
        tweets=tweets.union(ReadFile(path,fileName,fileName.split("_")(2),sc,sqlContext))
      }
    }

    var emojis = CountEmojiHelper(tweets, sqlContext)
  }

  def ReadFile(path:String,filename:String, feelingName: String, sc:SparkContext, sqlContext:SparkSession): DataFrame ={
    import sqlContext.implicits._
    val tokenized = sc.wholeTextFiles(path+filename)
      .map(t=>(feelingName,CleanTweet(t._2)))
      .toDF("FEELING","TWEETS")
    return tokenized
  }

  def CleanTweet(tweet:String):String={
    return tweet
  }

  def CountEmojiHelper(tweets:DataFrame, sqlContext:SparkSession): DataFrame = {
    import sqlContext.implicits._
    var emojis = tweets;
    emojis = emojis.withColumn("POS",CountEmoji(emojis("TWEETS"),lit("POS"))).withColumn("NEG",CountEmoji(emojis("TWEETS"),lit("NEG"))).withColumn("OTHERS",CountEmoji(emojis("TWEETS"),lit("OTHERS"))).withColumn("ADDITIONAL",CountEmoji(emojis("TWEETS"),lit("ADDITIONAL")))
    emojis.show()

    return emojis
  }

  def CountEmoji = udf((tweets: String, emojiType:String) => Option[Int] {
    var emojis = List[String]();
    if(emojiType == "POS") { emojis = UtilsPreProcessing.emojiPos }
    else if(emojiType == "NEG") { emojis = UtilsPreProcessing.emojiNeg }
    else if(emojiType == "OTHERS") { emojis = UtilsPreProcessing.othersEmoji }
    else if(emojiType == "ADDITIONAL") { emojis = UtilsPreProcessing.additionalEmoji }

    tweets.split(" ").filter(emoji => emojis.indexOf(emoji)!= -1).size
  })

  def GetFileList(path:String): List[String]={
    val d = new File(path)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).map(_.getName).toList
    } else {
      List[String]()
    }
  }
}
