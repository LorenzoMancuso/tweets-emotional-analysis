import java.util.{Properties}

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

private case class LexicalResOracle(var id:Long, var name:String, var value:Double, var idLemma:Long)
private case class LemmaOracle(var id:Long, var name:String, var percentage:Double=0, var idFeeling:Long)
private case class FeelingOracle(var id:Long, var name:String, var totalWords:Int=0)

object OracleUtils{

  def WriteToOracle(sc:SparkContext,df:DataFrame,emojis:DataFrame,hashtags:DataFrame) :Unit={
    //***ORACLE CONF***
    val sqlContext:SparkSession = SparkSession
      .builder()
      .appName("MAADB - progetto")
      .master("local[*]")
      .getOrCreate()

    Class.forName("oracle.jdbc.OracleDriver")
    val jdbcHostname = "127.0.0.1"
    val jdbcPort = 1521
    val jdbcUsername = "SYSTEM"
    val jdbcPassword = "root"

    // Create the JDBC URL without passing in the user and password parameters.
    val jdbcUrl = s"jdbc:oracle:thin:@${jdbcHostname}:${jdbcPort}:XE"
    // Create a Properties() object to hold the parameters.
    val connectionProperties = new Properties()
    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")

    val driverClass = "oracle.jdbc.OracleDriver"
    connectionProperties.setProperty("Driver", driverClass)
    //***END ORACLE CONF***

    val startTimeMillis = System.currentTimeMillis()
    //CALL 3 ORACLE INSERT WITH DIFFERENT QUERIES ON DATASET
    OracleInsert(df.select(df("FEELING").as("NAME")).distinct,"ALT_MAADB_FEELING",jdbcUrl,connectionProperties)
    OracleInsert(df.select(df("LEMMA").as("NAME"),df("PERCENTAGE"),df("FEELING"),df("FREQUENCY")).distinct,"ALT_MAADB_LEMMA",jdbcUrl,connectionProperties)
    OracleInsert(df.select(df("LEXICAL_RESOURCE").as("NAME"),df("COUNT").as("VALUE"),df("FEELING"),df("LEMMA")).distinct,"ALT_MAADB_LEXICAL_RESOURCE",jdbcUrl,connectionProperties)

    OracleInsert(emojis.select(emojis("FEELING"),emojis("SYMBOL"),emojis("ALIAS"),emojis("HTML_HEX"),emojis("COUNT")),"ALT_MAADB_SYMBOL",jdbcUrl,connectionProperties)
    OracleInsert(hashtags.select(hashtags("FEELING"),hashtags("LEMMA").as("HASHTAG"),hashtags("COUNT")),"ALT_MAADB_HASHTAG",jdbcUrl,connectionProperties)

    println("Elapsed time for Oracle write: ",(System.currentTimeMillis() - startTimeMillis) / 1000)
  }


  def OracleInsert(dataFrame: DataFrame, tableName:String, jdbcUrl:String, connectionProperties:Properties): Unit ={
    //println(dataFrame)
    dataFrame.repartition(10).write
      .mode("append")
      .jdbc(jdbcUrl, tableName, connectionProperties)
  }

}
