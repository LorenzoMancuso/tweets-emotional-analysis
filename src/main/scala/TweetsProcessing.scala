import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{DataFrame, SparkSession}

object TweetsProcessing{
  def Processing(lexicalRes:DataFrame,tweets:DataFrame,sc:SparkContext): Unit ={
    val sqlContext:SparkSession = SparkSession
      .builder()
      .appName("MAADB - progetto")
      .master("local[*]")
      .getOrCreate()

    import sqlContext.implicits._
    var splittedTweets:DataFrame=tweets.groupBy("FEELING","LEMMA").count()

    lexicalRes.createOrReplaceTempView("lexicalRes")
    splittedTweets.createOrReplaceTempView("splittedTweets")
    var result=sqlContext.sql(
      "SELECT DISTINCT lexicalRes.*,splittedTweets.COUNT AS FREQUENCY " +
        "FROM lexicalRes LEFT JOIN splittedTweets " +
        //"ON lexicalRes.FEELING in ('Pos','Neg','Like-Love','Hope') OR LOWER(lexicalRes.FEELING) LIKE LOWER(CONCAT('%',splittedTweets.FEELING,'%'))" + //controls on FEELING
        "ON LOWER(lexicalRes.FEELING) LIKE LOWER(CONCAT('%',splittedTweets.FEELING,'%'))" + //controls on FEELING
        "AND LOWER(lexicalRes.LEMMA) = LOWER(splittedTweets.LEMMA)" //controls on LEMMA
    )

    var newWord=sqlContext.sql(
      "SELECT DISTINCT splittedTweets.FEELING,splittedTweets.LEMMA, NULL AS LEXICAL_RESOURCE, NULL AS COUNT, NULL AS PERCENTAGE, splittedTweets.COUNT AS FREQUENCY " +
        "FROM splittedTweets LEFT JOIN lexicalRes ON LOWER(splittedTweets.LEMMA) = LOWER(lexicalRes.LEMMA) " +
        "WHERE lexicalRes.LEMMA is null AND LENGTH(splittedtweets.LEMMA) > 2 AND splittedTweets.COUNT>10 " +
        "ORDER BY 3 DESC"
    )

    result.printSchema()
    newWord.printSchema()

    result=result.union(newWord)
    //PrintToCSV(result)

    MongoUtils.WriteToMongo(sc,result)
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
