import java.io.File

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.lit
import org.apache.commons.lang.StringUtils
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
    println("read tweets ",path+filename)
    val tokenized = sc.wholeTextFiles(path+filename)
      .map(t=>(feelingName,CleanTweet(t._2:String)))
      .toDF("FEELING","TWEETS")

    tokenized.printSchema()
    return tokenized
  }

  def CleanTweet(tweet:String):String={
    var res=tweet
    //eliminare punteggiatura
    res=StringUtils.replaceEach(res, UtilsPreProcessing.punctuaction.toArray, Array.fill[String](UtilsPreProcessing.punctuaction.length)(""))
    res=res.split(" ")
      .filter(word=> !word.contains("USERNAME") && !word.contains("URL") && UtilsPreProcessing.stopWords.indexOf(word) == -1) //eliminare stop words
      .map(word=>UtilsPreProcessing.slang.getOrElse(word,word)) //sostituire slang
      .mkString(" ").trim().replaceAll(" +", " ")
    return res
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

  def PrintToCSV(df: DataFrame): Unit ={
    println("start writing")
    df.coalesce(1)         // Writes to a single file
      .write
      .mode("overwrite")
      .format("csv")
      .save("./RESULTS/")
  }
}
