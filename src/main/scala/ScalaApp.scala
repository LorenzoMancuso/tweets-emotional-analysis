import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.File
import java.util.Date

import org.apache.spark.sql.{DataFrame, SparkSession}
// PrintWriter
import java.io._
import MongoUtils._
import OracleUtils._

object ScalaApp {
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val sparkConf=new SparkConf()
      .setAppName("MAADB - progetto")
      .setMaster("local[*]")
      .set("spark.executor.memomory","10g")
      .set("spark.driver.memomory","10g")
      .set("spark.executor.memoryOverhead","10g")
      .set("spark.memory.offHeap.enabled","true")
      .set("spark.memory.offHeap.size","10g")
      .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/MAADB.lexical_res_alts")
      .set("spark.mongodb.output.uri", "mongodb://127.0.0.1/MAADB.lexical_res_alts")
    //--master yarn-cluster --num-executors 10 --executor-cores 3 --executor-memory 4g --driver-memory 5g  --conf spark.yarn.executor.memoryOverhead=409

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")

    //UtilsPreProcessing.PreProcessing(sc)
    //println("Utils preprocessing done")
    /*println(UtilsPreProcessing.emojiPos)
    println(UtilsPreProcessing.emojiNeg)
    println(UtilsPreProcessing.othersEmoji)*/

    val lexicalRes=LexicalResPreProcessingAlt.PreProcessing(sc)
    println("Lexical Res preprocessing done")

    val (tweets,emojis,hashtags)=TweetsPreProcessing.PreProcessing(sc)
    println("Tweets preprocessing done")

    val result=TweetsProcessing.Processing(lexicalRes,tweets,sc)
    println("Final data processing done")

    WriteToMongo(sc,result,emojis,hashtags)
    println("write to Mongo executed")

    /*WriteToOracle(sc,result,emojis,hashtags)
    println("write to Oracle executed")*/

  }

}
