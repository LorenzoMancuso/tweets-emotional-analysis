# MAADB-scala_spark #

Second part of MAADB project.

Necessary Software:
* scala 2.11.x
* spark-shell 2.3.x
* mongodb 3.6.x
* sbt 1.2.x

## Important ##git add .

In build.sbt a library dependence is add to satisfy the spark inclusion in sbt, 
but, if you have the spark version 2.3.1, you'd must set into dependence :
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
 