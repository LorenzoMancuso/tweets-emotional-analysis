import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.File
import java.util.Date
// PrintWriter
import java.io._

import LexicalResPreProcessing._
import MongoUtils._

class LexicalResource(var name:String, var occurrences:Int=1)
class Lemma(var name:String, var lexicalRes:List[LexicalResource]=List[LexicalResource](), var percentage:Double=0)
class Feeling(var name:String, var lemmas:List[Lemma]=List[Lemma](), var totalWords:Int=0)

object ScalaApp {
  def main(args: Array[String]) {

    // create Spark context with Spark configuration
    val sparkConf=new SparkConf()
      .setAppName("MAADB - progetto")
      .setMaster("local[*]")
      .set("spark.executor.memomory","1G")
      .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/MAADB.lexical_res")
      .set("spark.mongodb.output.uri", "mongodb://127.0.0.1/MAADB.lexical_res")

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")

    val preprocessedLexicalRes=PreProcessing(sc)

    WriteToMongo(sc,preprocessedLexicalRes)
    println("write to Mongo executed")
  }

}
