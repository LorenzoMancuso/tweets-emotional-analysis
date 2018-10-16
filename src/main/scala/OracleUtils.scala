import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
object OracleUtils{

  def WriteToOracle(sc:SparkContext,feelingList:List[Feeling]) :Unit={
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
    //val jdbcUrl = s"jdbc:oracle:thin@${jdbcHostname}:${jdbcPort}:XE;database=${jdbcDatabase}"
    val jdbcUrl = s"jdbc:oracle:thin:@${jdbcHostname}:${jdbcPort}:XE"
    //val jdbcUrl = "jdbc:oracle:thin:@127.0.0.1:1521:XE"


    // Create a Properties() object to hold the parameters.
    val connectionProperties = new Properties()

    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")

    val driverClass = "oracle.jdbc.OracleDriver"
    connectionProperties.setProperty("Driver", driverClass)

    import sqlContext.implicits._
    val Feeling_df:DataFrame = feelingList.toDF()

    Feeling_df.write
    .mode("Overwrite")
    .jdbc(jdbcUrl, "lexical_resource", connectionProperties)
  }

}
