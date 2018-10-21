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
    sqlContext.sql("select FEELING,LEMMA,PERCENTAGE, collect_list(struct(LEXICAL_RESOURCE,count)) as lexicalRes from tmp group by FEELING,LEMMA,PERCENTAGE").createOrReplaceTempView("tmp")
    val res=sqlContext.sql("select FEELING, collect_list(struct(LEMMA,PERCENTAGE,lexicalRes)) as lemmas from tmp group by FEELING").toJSON.collect

    sparkContext.parallelize(res.map(Document.parse)).saveToMongoDB()
  }
}