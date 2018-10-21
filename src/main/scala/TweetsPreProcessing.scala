import java.io.File

import LexicalResPreProcessingAlt.CleanLRName
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
  }

  def ReadFile(path:String,filename:String, feelingName: String, sc:SparkContext, sqlContext:SparkSession): DataFrame ={
    import sqlContext.implicits._
    val tokenized = sc.textFile(path+filename)
      .map(t=>(feelingName,CleanTweet(t)))
      .toDF("FEELING","TWEETS")
    return tokenized
  }

  def CleanTweet(tweet:String):String={
    return tweet
  }

  def GetFileList(path:String): List[String]={
    val d = new File(path)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).map(_.getName).toList
    } else {
      List[String]()
    }
  }
}
