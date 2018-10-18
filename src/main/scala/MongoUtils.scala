import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import org.apache.spark.SparkContext
import org.bson.Document
import net.liftweb.json._
import net.liftweb.json.Serialization.write

object MongoUtils{

  def WriteToMongo(sparkContext:SparkContext,feelingList:List[Feeling]): Unit ={
    // create a JSON string from the Person, then print it
    implicit val formats = DefaultFormats

    var jsonString = write(feelingList(0)).trim.stripMargin.split("[\\r\\n]+").toSeq
    for( a <- 1 to 10){
      jsonString ++= write(feelingList(a)).trim.stripMargin.split("[\\r\\n]+").toSeq
    }

    sparkContext.parallelize(jsonString.map(Document.parse)).saveToMongoDB()
  }
}