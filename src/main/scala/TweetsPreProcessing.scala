import java.io.File

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.explode
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.vdurmont.emoji._

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

    import sqlContext.implicits._
    //sostituire slang
    tweets=tweets.map(row=>(row.getString(0),UtilsPreProcessing.slang.getOrElse(row.getString(1),row.getString(1).toLowerCase).toLowerCase)).toDF()

    //tokenized tweets
    var splittedTweets=tweets.map(row=>(row.getString(0),row.getString(1).split(" "))).toDF("FEELING","TWEETS_WORDS_LIST")
    splittedTweets=splittedTweets.withColumn("LEMMA", explode(splittedTweets("TWEETS_WORDS_LIST"))).drop("TWEETS_WORDS_LIST")

    //rimuove parole anonimizzate USERNAME e URL
    splittedTweets=splittedTweets.filter(word=> !word.getString(1).contains("USERNAME") && !word.getString(1).contains("URL"))

    var hashtags = splittedTweets.filter(_.getString(1).contains("#"))
    //remove all hashtags
    splittedTweets=splittedTweets.except(hashtags)
    hashtags=hashtags.groupBy("FEELING","LEMMA").count()

    //count emojis and emoticons
    var emojis = CountEmojiHelper(tweets.toDF("FEELING","TWEETS"), sqlContext)

    //remove all emoticons
    splittedTweets=splittedTweets.filter(row=>(UtilsPreProcessing.posemoticons.indexWhere(_.contains(row.getString(1)))== -1
      && UtilsPreProcessing.negemoticons.indexWhere(_.contains(row.getString(1)))== -1))

    // clean emoji, punctuation, stop words and spaces - filter for empty string
    splittedTweets=splittedTweets.as[(String,String)].map(t=>(t._1,CleanTweet(t._2:String))).filter(_._2!="").toDF("FEELING","LEMMA")

    return splittedTweets
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
    tweets.split(" ").count(emoji => emojis.indexWhere(_.contains(emoji))!= -1)//indexOf with like clause
  })

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
    res=EmojiParser.removeAllEmojis(res)
    res=StringUtils.replaceEach(res, UtilsPreProcessing.punctuaction.toArray, Array.fill[String](UtilsPreProcessing.punctuaction.length)(" "))//eliminare punteggiatura
    if(UtilsPreProcessing.stopWords.indexOf(res) != -1) {res=""}//eliminare stop words
    res=res.replaceAll(" ","").replaceAll("\n","")
    return res
  }

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
