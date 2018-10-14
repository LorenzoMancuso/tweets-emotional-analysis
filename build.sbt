name := "MAADB - progetto"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.2.1"
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.4.2"
)