import java.io.File

import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

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
    //PrintToCSV(tweets)
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
