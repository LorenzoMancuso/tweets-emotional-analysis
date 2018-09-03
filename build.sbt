name := "MAADB-scala_spark"

version := "0.1"

scalaVersion := "2.11.11"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"

// https://mvnrepository.com/artifact/com.stratio.datasource/spark-mongodb
libraryDependencies += "com.stratio.datasource" %% "spark-mongodb" % "0.12.0"

// https://mvnrepository.com/artifact/org.mongodb/bson
libraryDependencies += "org.mongodb" % "bson" % "3.6.3"

// https://mvnrepository.com/artifact/org.mongodb.scala/mongo-scala-driver
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.4.1"


scalacOptions += "-deprecation"

