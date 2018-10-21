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
  }

  def WriteToOracleOld(sc:SparkContext,feelingList:List[Feeling]) :Unit={

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

    FeelingToOracleObject(sqlContext,feelingList,jdbcUrl,connectionProperties)

  }

  def FeelingToOracleObject(sqlContext:SparkSession, feelingList:List[Feeling], jdbcUrl:String, connectionProperties:Properties): Unit ={
    import sqlContext.implicits._
    println("Started FeelingObjectToOracle: ",Calendar.getInstance())

    var tmpLexRes=List[LexicalResOracle]()
    var tmpLemma=List[LemmaOracle]()
    var tmpFeeling=List[FeelingOracle]()

    feelingList.foreach{f=>
      tmpFeeling=new FeelingOracle(f.id,f.name,f.totalWords)::tmpFeeling
      f.lemmas.foreach{l=>
        tmpLemma=new LemmaOracle(l.id,l.name,l.percentage,f.id)::tmpLemma
        //wtite lemma in table with feeling id
        l.lexicalRes.foreach(lr=>tmpLexRes=new LexicalResOracle(lexResIndex(),lr._1,lr._2,l.id)::tmpLexRes)
      }
    }
    println(tmpLexRes.length, tmpLexRes(0))
    println("Finished FeelingObjectToOracle: ",Calendar.getInstance())

    OracleInsert(tmpFeeling.toDF("ID","NAME","TOTAL_WORDS"),"SYSTEM.MAADB_FEELING", jdbcUrl,connectionProperties)
    OracleInsert(tmpLemma.toDF("ID","NAME","PERCENTAGE","ID_FEELING"), "SYSTEM.MAADB_LEMMA", jdbcUrl,connectionProperties)
    OracleInsert(tmpLexRes.toDF("ID","NAME","VALUE","ID_LEMMA"), "SYSTEM.MAADB_LEXICAL_RESOURCE", jdbcUrl,connectionProperties)

    //val Feeling_df:DataFrame = feelingList.toDF()


  }

  def OracleInsert(dataFrame: DataFrame, tableName:String, jdbcUrl:String, connectionProperties:Properties): Unit ={
    //println(dataFrame)
    dataFrame.repartition(10).write
      .mode("append")
      .jdbc(jdbcUrl, tableName, connectionProperties)
  }

}
