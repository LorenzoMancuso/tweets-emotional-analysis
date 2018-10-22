import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{DataFrame, SparkSession}

object TweetsProcessing{
  def Processing(lexicalRes:DataFrame,tweets:DataFrame): Unit ={
    val sqlContext:SparkSession = SparkSession
      .builder()
      .appName("MAADB - progetto")
      .master("local[*]")
      .getOrCreate()

    lexicalRes.printSchema()
    tweets.printSchema()

    import sqlContext.implicits._
    var splittedTweets=tweets.map(row=>(row.getString(0),row.getString(1).split(" ").filter(word=> !word.contains("#") && !word.contains("[^\u0000-\uFFFF]")))).toDF("FEELING","TWEETS_WORDS_LIST")//[^\u0000-\uFFFF]
    splittedTweets=splittedTweets.withColumn("LEMMA", explode(splittedTweets("TWEETS_WORDS_LIST"))).drop("TWEETS_WORDS_LIST").groupBy("FEELING","LEMMA").count()

    lexicalRes.createOrReplaceTempView("lexicalRes")
    splittedTweets.createOrReplaceTempView("splittedTweets")
    var result=sqlContext.sql(
      "SELECT DISTINCT lexicalRes.*,splittedTweets.COUNT AS FREQUENCY " +
        "FROM lexicalRes LEFT JOIN splittedTweets " +
        "ON lexicalRes.FEELING in ('Pos','Neg','Like-Love','Hope') OR LOWER(lexicalRes.FEELING) LIKE LOWER(CONCAT('%',splittedTweets.FEELING,'%'))" + //controls on FEELING
        "AND LOWER(lexicalRes.LEMMA) = LOWER(splittedTweets.LEMMA)" //controls on LEMMA
    )

    var newWord=sqlContext.sql(
      "SELECT DISTINCT splittedTweets.FEELING,splittedTweets.LEMMA,splittedTweets.COUNT AS FREQUENCY " +
        "FROM splittedTweets LEFT JOIN lexicalRes ON LOWER(splittedTweets.LEMMA) = LOWER(lexicalRes.LEMMA) " +
        "WHERE lexicalRes.LEMMA is null " +
        "ORDER BY 3 DESC"
    )
    newWord.show()
  }
}
