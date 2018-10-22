import java.io.File

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.explode
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object TweetsPreProcessing{

  def PreProcessing(sc:SparkContext): DataFrame ={
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
    
    println("count hashtags")
    var hashtags:DataFrame = CountHashtagsHelper(tweets, sqlContext, sc)

    println("count emojis")
    var emojis = CountEmojiHelper(tweets, sqlContext)

    // clean punctuation
    import sqlContext.implicits._
    tweets=tweets.as[(String,String)].map(t=>(t._1,CleanTweet(t._2:String))).toDF


    return tweets.toDF("FEELING","TWEETS")
  }

  def CountHashtagsHelper(tweets:DataFrame, sqlContext:SparkSession, sc:SparkContext): DataFrame = {
    import sqlContext.implicits._

    var hashtags = tweets.map(row=>(row.getString(0),row.getString(1).split(" ").filter(_.contains("#")))).toDF("FEELING","HASHTAGS_LIST")
    hashtags=hashtags.withColumn("HASHTAG", explode(hashtags("HASHTAGS_LIST"))).drop("HASHTAGS_LIST").groupBy("FEELING","HASHTAG").count()

    return hashtags
  }

  def ReadFile(path:String,filename:String, feelingName: String, sc:SparkContext, sqlContext:SparkSession): DataFrame ={
    import sqlContext.implicits._
    println("read tweets ",path+filename)
    val tokenized = sc.wholeTextFiles(path+filename)
      .map(t=>(feelingName,t._2:String))
      .toDF("FEELING","TWEETS")
    return tokenized
  }

  def CleanTweet(tweet:String):String={
    var res=tweet
    res=StringUtils.replaceEach(res, UtilsPreProcessing.punctuaction.toArray, Array.fill[String](UtilsPreProcessing.punctuaction.length)(" "))//eliminare punteggiatura
    res=res.split(" ")
      .filter(word=> !word.contains("USERNAME") && !word.contains("URL") /*&& UtilsPreProcessing.stopWords.indexOf(word) == -1*/) //eliminare parole anonimizzate USERNAME e URL
      .map(word=>UtilsPreProcessing.slang.getOrElse(word,word.toLowerCase).toLowerCase)//sostituire slang
      .filter(UtilsPreProcessing.stopWords.indexOf(_) == -1) //eliminare stop words
      .mkString(" ").trim().replaceAll(" +", " ") //remove double spaces
    return res
  }

  def CountEmojiHelper(tweets:DataFrame, sqlContext:SparkSession): DataFrame = {
    var emojis = tweets
    emojis = emojis
      .withColumn("POS",CountEmoji(emojis("TWEETS"),lit("POS")))
      .withColumn("NEG",CountEmoji(emojis("TWEETS"),lit("NEG")))
      .withColumn("OTHERS",CountEmoji(emojis("TWEETS"),lit("OTHERS")))
      .withColumn("ADDITIONAL",CountEmoji(emojis("TWEETS"),lit("ADDITIONAL")))
      .withColumn("EMOTICON_POS",CountEmoji(emojis("TWEETS"),lit("EMO_POS")))
      .withColumn("EMOTICON_NEG",CountEmoji(emojis("TWEETS"),lit("EMO_NEG")))
    return emojis
  }

  def CountEmoji = udf((tweets: String, emojiType:String) => Option[Int] {
    var emojis = List[String]()
    if(emojiType == "POS") { emojis = UtilsPreProcessing.emojiPos }
    else if(emojiType == "NEG") { emojis = UtilsPreProcessing.emojiNeg }
    else if(emojiType == "OTHERS") { emojis = UtilsPreProcessing.othersEmoji }
    else if(emojiType == "ADDITIONAL") { emojis = UtilsPreProcessing.additionalEmoji }
    else if(emojiType == "EMO_POS") { emojis = UtilsPreProcessing.posemoticons}
    else if(emojiType == "EMO_NEG") { emojis = UtilsPreProcessing.negemoticons }
    tweets.split(" ").count(emoji => emojis.indexWhere(_.contains(emoji))!= -1)
  })

  def GetFileList(path:String): List[String]={
    val d = new File(path)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).map(_.getName).toList
    } else {
      List[String]()
    }
  }

  def PrintToCSV(df: DataFrame): Unit ={
    println("start writing")
    df.coalesce(1)         // Writes to a single file
      .write
      .mode("overwrite")
      .format("csv")
      .save("./RESULTS/")
  }
}
