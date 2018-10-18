import java.io.File
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object LexicalResPreProcessingAlt{

  def PreProcessingAlt(sc: SparkContext): Unit ={
    val sqlContext:SparkSession = SparkSession
      .builder()
      .appName("MAADB - progetto")
      .master("local[*]")
      .getOrCreate()

    var df:DataFrame=null
    val path:String="./DATASET/Risorse-lessicali/"

    //get lemmas list with occurrences
    GetListOfSubDirectories(path).foreach{feeling=>
      GetFileList(path + feeling).foreach { lr =>
        if (df == null) {
          df = ReadFile(path, lr, feeling, sc, sqlContext)
        } else {
          if(feeling=="ConScore") {
            df = df.union(ReadScores(path, lr, feeling, sc, sqlContext))
          }else {
            df = df.union(ReadFile(path, lr, feeling, sc, sqlContext))
          }
        }
      }
    }
    //df.createOrReplaceTempView("tmp")
    //df=sqlContext.sql("SELECT *, COUNT(*) AS TOTAL")
    PrintToCSV(df)
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
      .map(x=>(feelingName,x.split("\t")(0),CleanLRName(filename),x.split("\t")(1)))
      .toDF("FEELING","LEMMA","LEXICAL_RESOURCE","COUNT")
    return tokenized
  }

  def CleanLRName(name:String):String={
    var res=name.replaceAll("(\\.[^\\.]*$)", "")
    if(!res.contains("POS") && !res.contains("NEG") )
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
