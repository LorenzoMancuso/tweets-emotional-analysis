import java.util.{Calendar, Properties}

import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

private case class LexicalResOracle(var id:Long, var name:String, var value:Double, var idLemma:Long)
private case class LemmaOracle(var id:Long, var name:String, var percentage:Double=0, var idFeeling:Long)
private case class FeelingOracle(var id:Long, var name:String, var totalWords:Int=0)

object OracleUtils extends indexes {

  def WriteToOracle(sc:SparkContext,df:DataFrame) :Unit={
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

    //CALL 3 ORACLE INSERT WITH DIFFERENT QUERIES ON DATASET
    OracleInsert(df.select(df("FEELING"),df("TOTAL")).distinct,"ALT_MAADB_FEELING",jdbcUrl,connectionProperties)
    OracleInsert(df.select(df("LEMMA"),df("PERCENTAGE"),df("FEELING")).distinct,"ALT_MAADB_FEELING",jdbcUrl,connectionProperties)
    OracleInsert(df.select(df("LEXICAL_RESOURCE"),df("COUNT"),df("FEELING"),df("LEMMA")).distinct,"ALT_MAADB_FEELING",jdbcUrl,connectionProperties)

  }


  def OracleInsert(dataFrame: DataFrame, tableName:String, jdbcUrl:String, connectionProperties:Properties): Unit ={
    //println(dataFrame)
    dataFrame.repartition(10).write
      .mode("append")
      .jdbc(jdbcUrl, tableName, connectionProperties)
  }

}
