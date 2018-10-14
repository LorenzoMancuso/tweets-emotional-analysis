import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import org.apache.spark.SparkContext
import org.bson.Document


object MongoUtils{

  def WriteToMongo(sparkContext:SparkContext): Unit ={

    val docs = """
      {"name": "Bilbo Baggins", "age": 50}
      {"name": "Gandalf", "age": 1000}
      {"name": "Thorin", "age": 195}
      {"name": "Balin", "age": 178}
      {"name": "Kíli", "age": 77}
      {"name": "Dwalin", "age": 169}
      {"name": "Óin", "age": 167}
      {"name": "Glóin", "age": 158}
      {"name": "Fíli", "age": 82}
      {"name": "Bombur"}""".trim.stripMargin.split("[\\r\\n]+").toSeq
    sparkContext.parallelize(docs.map(Document.parse)).saveToMongoDB()
  }

  /*def personToDocument(person: Person): Document = {
    Document("age" -> person.age, "name" -> person.name)
  }*/
}