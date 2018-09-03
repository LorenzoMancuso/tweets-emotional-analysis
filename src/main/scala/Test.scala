import org.mongodb.scala._

object App{

  def main(args:Array[String]){

    val connectionString: String = "mongodb://magno:labdb@localhost/?authSource=labDB"
    val mongoClient: MongoClient = MongoClient(/*connectionString*/)
    val database: MongoDatabase = mongoClient.getDatabase("labDB")
    val collection: MongoCollection[Document] = database.getCollection("test")
    val doc: Document = Document(
      "_id" -> 0,
      "name" -> "MongoDB",
      "type" -> "database",
      "count" -> 1,
      "prova" -> Document("id" -> 1, "name" -> "magno")
    )

    println(collection)
  }
}

