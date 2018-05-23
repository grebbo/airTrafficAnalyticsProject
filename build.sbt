name := "AirDelaysPreProcessing"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVersion = "2.1.1"
  val connectorVersion = "2.0.1"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
    "com.datastax.spark" %% "spark-cassandra-connector" % connectorVersion % "provided"
  )
}