import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mongodb.spark._
import org.apache.spark.SparkContext
import org.bson.Document
import net.liftweb.json._
import net.liftweb.json.Serialization.write

object MongoUtils{
  /*
 root
 |-- _id: string (nullable = true)
 |-- lemmas: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- LEMMA: string (nullable = true)
 |    |    |-- PERCENTAGE: double (nullable = true)
 |    |    |-- FREQUENCY: long (nullable = true)
 |    |    |-- lexicalRes: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- LEXICAL_RESOURCE: string (nullable = true)
 |    |    |    |    |-- count: string (nullable = true)

root
 |-- _id: string (nullable = true)
 |-- emojis: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- SYMBOL: string (nullable = true)
 |    |    |-- ALIAS: string (nullable = true)
 |    |    |-- HTML_HEX: string (nullable = true)
 |    |    |-- COUNT: long (nullable = false)

root
 |-- _id: string (nullable = true)
 |-- hashtags: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- HASHTAG: string (nullable = true)
 |    |    |-- COUNT: long (nullable = false)
 */
  def WriteToMongo(sparkContext:SparkContext,df:DataFrame,emojis:DataFrame,hashtags:DataFrame): Unit ={
    val sqlContext:SparkSession = SparkSession
      .builder()
      .appName("MAADB - progetto")
      .master("local[*]")
      .getOrCreate()

    import sqlContext.implicits._
    df.createOrReplaceTempView("tmp")
    emojis.createOrReplaceTempView("EMOJIS")
    hashtags.createOrReplaceTempView("HASHTAGS")

    sqlContext.sql("select tmp.FEELING,tmp.LEMMA,tmp.PERCENTAGE,tmp.FREQUENCY, " +
        "collect_list(struct(tmp.LEXICAL_RESOURCE,tmp.count)) as lexicalRes " +
      "from tmp " +
        "group by tmp.FEELING,tmp.LEMMA,tmp.PERCENTAGE,tmp.FREQUENCY").createOrReplaceTempView("tmp")

    /*val res=sqlContext.sql("select tmp.FEELING, collect_list(struct(tmp.LEMMA,tmp.PERCENTAGE,tmp.FREQUENCY,tmp.lexicalRes)) as lemmas, " +
      "collect_list(struct(EMOJIS.SYMBOL, EMOJIS.ALIAS, EMOJIS.HTML_HEX)) as emojis, " +
      "collect_list(struct(HASHTAGS.LEMMA)) as hashtags " +
      "from tmp JOIN EMOJIS ON tmp.FEELING=EMOJIS.FEELING " +
      "JOIN HASHTAGS ON tmp.FEELING=HASHTAGS.FEELING " +
      "WHERE tmp.FEELING='joy' " +
      "group by tmp.FEELING")
    res.printSchema()*/

    val mongoLemmas=sqlContext.sql("select tmp.FEELING as _id, collect_list(struct(tmp.LEMMA,tmp.PERCENTAGE,tmp.FREQUENCY,tmp.lexicalRes)) as lemmas " +
      "from tmp " +
      "group by tmp.FEELING")
    mongoLemmas.printSchema()

    val mongoLemmas1=mongoLemmas.filter(row=> Array("joy","anger","surprise").contains(row.getString(0)))
    val mongoLemmas2=mongoLemmas.except(mongoLemmas1)

    val mongoEmojis=sqlContext.sql("select EMOJIS.FEELING as _id, collect_list(struct(EMOJIS.SYMBOL, EMOJIS.ALIAS, EMOJIS.HTML_HEX, EMOJIS.COUNT)) as emojis " +
      "from EMOJIS " +
      "group by EMOJIS.FEELING")
    mongoEmojis.printSchema()

    val mongoHashtags=sqlContext.sql("select HASHTAGS.FEELING as _id, collect_list(struct(HASHTAGS.LEMMA AS HASHTAG, HASHTAGS.COUNT)) as hashtags " +
      "from HASHTAGS " +
      "group by HASHTAGS.FEELING")
    mongoHashtags.printSchema()

    sqlContext.catalog.dropTempView("tmp")
    sqlContext.catalog.dropTempView("EMOJIS")
    sqlContext.catalog.dropTempView("HASHTAGS")


    val startTimeMillis = System.currentTimeMillis()
    MongoSpark.save(mongoLemmas1.write.option("replaceDocument", "false").mode("append")) //mode("append")
    MongoSpark.save(mongoLemmas2.write.option("replaceDocument", "false").mode("append")) //mode("append")

    MongoSpark.save(mongoEmojis.write.option("replaceDocument", "false").mode("append")) //mode("append")
    MongoSpark.save(mongoHashtags.write.option("replaceDocument", "false").mode("append")) //mode("append")
    println("Elapsed time for Mongo write: ",(System.currentTimeMillis() - startTimeMillis) / 1000)
  }
}