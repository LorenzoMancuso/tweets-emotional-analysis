import java.io.File
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit

object LexicalResPreProcessingAlt{

  /*root
     |-- FEELING: string (nullable = true)
     |-- LEMMA: string (nullable = true)
     |-- LEXICAL_RESOURCE: string (nullable = true)
     |-- count: long (nullable = false)
     |-- TOTAL: long (nullable = true)
     |-- TOTAL_LEMMA: long (nullable = true)
     |-- PERCENTAGE: double (nullable = true)*/

  def PreProcessingAlt(sc: SparkContext): DataFrame ={
    val sqlContext:SparkSession = SparkSession
      .builder()
      .appName("MAADB - progetto")
      .master("local[*]")
      .getOrCreate()

    var df:DataFrame=null
    val path:String="./DATASET/Risorse-lessicali/"

    //get lemmas list with occurrences
    GetListOfSubDirectories(path).foreach{feeling=>
      if(feeling!="ConScore"){
        GetFileList(path + feeling).foreach { lr =>
          if (df == null) {
            df = ReadFile(path, lr, feeling, sc, sqlContext)
          } else {
            df = df.union(ReadFile(path, lr, feeling, sc, sqlContext))
          }
        }
      }
    }

    //***ADD PERCENTAGE***
    df.createOrReplaceTempView("tmp")
    var tmp1=sqlContext.sql("SELECT FEELING, SUM(COUNT) AS TOTAL FROM tmp GROUP BY FEELING")
    var tmp2=sqlContext.sql("SELECT FEELING, LEMMA, SUM(COUNT) AS TOTAL_LEMMA FROM tmp GROUP BY FEELING,LEMMA")
    var joined=tmp1.join(tmp2,Seq("FEELING"))
    df=df.join(joined,Seq("FEELING","LEMMA")).withColumn("PERCENTAGE", joined("TOTAL_LEMMA")/joined("TOTAL")).drop("TOTAL").drop("TOTAL_LEMMA")

    //***ADD SCORES***
    var scores:DataFrame=null
    GetFileList(path + "ConScore").foreach { lr =>
      if (scores == null) {
        scores = ReadScores(path, lr, "ConScore", sc, sqlContext)
      } else {
        scores = scores.union(ReadScores(path, lr, "ConScore", sc, sqlContext))
      }
    }

    //println("schema with scores")
    scores=df.join(scores,"LEMMA").select(df("FEELING"),df("LEMMA"),scores("LEXICAL_RESOURCE"),scores("COUNT"),df("PERCENTAGE"))
    df=df.union(scores)

    //df.printSchema()
    return df
    //PrintToCSV(df)
  }

  def GetListOfSubDirectories(directoryName: String): Array[String] = {
    new File(directoryName)
      .listFiles
      .filter(_.isDirectory)
      .map(_.getName)
  }

  def GetFileList(path:String): List[String]={
    val d = new File(path)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).map(_.getName).toList
    } else {
      List[String]()
    }
  }

  def ReadFile(path:String,filename:String, feelingName: String, sc:SparkContext, sqlContext:SparkSession): DataFrame ={
    import sqlContext.implicits._
    val tokenized = sc.textFile(path+feelingName+"/"+filename)
      .flatMap(_.split(" "))
      .map((feelingName,_,CleanLRName(filename)))
      .toDF("FEELING","LEMMA","LEXICAL_RESOURCE")
      .filter(!$"LEMMA".contains("_"))
      .groupBy("FEELING","LEMMA","LEXICAL_RESOURCE")
      .count()
    return tokenized
  }

  def ReadScores(path:String,filename:String, feelingName: String, sc:SparkContext, sqlContext:SparkSession): DataFrame ={
    import sqlContext.implicits._
    val tokenized = sc.textFile(path+feelingName+"/"+filename)
      .flatMap(_.split("\n"))
      .map(x=>(feelingName,x.split("\t")(0),CleanScoreName(filename),x.split("\t")(1)))
      .toDF("FEELING","LEMMA","LEXICAL_RESOURCE","COUNT")
    return tokenized
  }

  def CleanLRName(name:String):String={
    var res=name.replaceAll("(\\.[^\\.]*$)", "")
    if(!res.contains("POS") && !res.contains("NEG") )
      res=res.replaceAll("(\\_[^\\_]*$)", "")
    return res
  }

  def CleanScoreName(name:String):String={
    var res=name.replaceAll("(\\.[^\\.]*$)", "")
    if(res.contains("tab"))
      res=res.replaceAll("(\\_[^\\_]*$)", "")
    return res
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
