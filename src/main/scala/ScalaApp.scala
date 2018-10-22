import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.File
import java.util.Date

import org.apache.spark.sql.SparkSession
// PrintWriter
import java.io._
import MongoUtils._
import OracleUtils._

trait indexes {
  val lexResIndex={ var i :Long= 0; () => { i += 1; i} }
  val lemmaIndex={ var i :Long= 0; () => { i += 1; i} }
  val feelingIndex={ var i :Long= 0; () => { i += 1; i} }
}

case class Lemma(var id:Long, var name:String, var lexicalRes:scala.collection.mutable.Map[String,Double]=scala.collection.mutable.Map[String,Double](), var percentage:Double=0)
case class Feeling(var id:Long, var name:String, var lemmas:List[Lemma]=List[Lemma](), var totalWords:Int=0)

object ScalaApp {
  def main(args: Array[String]) {

    // create Spark context with Spark configuration
    val sparkConf=new SparkConf()
      .setAppName("MAADB - progetto")
      .setMaster("local[*]")
      .set("spark.executor.memomory","1G")
      .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/MAADB.lexical_res_alt")
      .set("spark.mongodb.output.uri", "mongodb://127.0.0.1/MAADB.lexical_res_alt")

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")

    UtilsPreProcessing.PreProcessing(sc)

    val lexicalRes=LexicalResPreProcessingAlt.PreProcessing(sc)
    val tweets=TweetsPreProcessing.PreProcessing(sc)

    TweetsProcessing.Processing(lexicalRes,tweets)

    //WriteToMongo(sc,preprocessedLexicalRes)
    //println("write to Mongo executed")

    //WriteToOracle(sc,preprocessedLexicalRes)
    //println("write to Oracle executed")
  }

}
