import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mongodb.spark._
import org.apache.spark.SparkContext
import org.bson.Document
import net.liftweb.json._
import net.liftweb.json.Serialization.write

object MongoUtils{

  def WriteToMongo(sparkContext:SparkContext,df:DataFrame): Unit ={
    val sqlContext:SparkSession = SparkSession
      .builder()
      .appName("MAADB - progetto")
      .master("local[*]")
      .getOrCreate()
    import sqlContext.implicits._
    df.createOrReplaceTempView("tmp")
    sqlContext.sql("select FEELING,LEMMA,PERCENTAGE,FREQUENCY, collect_list(struct(LEXICAL_RESOURCE,count)) as lexicalRes from tmp group by FEELING,LEMMA,PERCENTAGE,FREQUENCY").createOrReplaceTempView("tmp")
    val res=sqlContext.sql("select FEELING, collect_list(struct(LEMMA,PERCENTAGE,FREQUENCY,lexicalRes)) as lemmas from tmp group by FEELING")
    res.printSchema()

    /*val tmp=res.toJSON.collect
    println("DataFrame parsed to JSON array")
    val doc=tmp.map(Document.parse)
    println("JSON array parsed to BSON Document")
    sparkContext.parallelize(doc).saveToMongoDB()*/

    val startTimeMillis = System.currentTimeMillis()
    MongoSpark.save(res.write.mode("overwrite"))
    println("Elapsed time for Mongo write: ",(System.currentTimeMillis() - startTimeMillis) / 1000)
  }
}