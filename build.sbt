ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "BDProjectV2"
  )

val sparkVersion = "3.4.2"

libraryDependencies ++= Seq(

  // Spark Core & SQL
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // Structured Streaming + Kafka
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,

  // MongoDB Spark Connector
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.1.1",

  // MLlib (optional)
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)
