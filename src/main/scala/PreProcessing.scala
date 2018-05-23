import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.{SparkSession, functions}
import scala.collection.mutable.ListBuffer

object PreProcessing {
  def main(args: Array[String]): Unit = {

    // create SparkSession object with Cassandra connector host
    val spark = SparkSession.builder().
      master("yarn").
      appName("AirTrafficAnalytics").
      config("spark.cassandra.connection.host", "slave-1.localdomain").
      getOrCreate()
    import spark.implicits._

    // val cassandraTableName = if (args.length == 0) "history" else args(0)

    // categorical features list
    val categoricalVariables = ListBuffer[String](
      "origin_icao",
      "destination_icao",
      "airline",
      "flight_number",
      "full_aircrafttype")

    // import data from Cassandra
    var airTrafficData = spark.read.
      format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "history", "keyspace" -> "flight_aware")).
      load()

    // TODO: select condition for landed flights
    val maxProgressDf = airTrafficData.filter($"progress" === 100)

    // alternative criteria for selection: take only the most recent one (max progress feature)
    /*
    val maxProgressDf = airTrafficData.
      groupBy($"faflightid").
      agg(functions.max("progress").alias("max_progress")).
      withColumnRenamed("faflightid", "flightid_max_progress")
      */

    airTrafficData = maxProgressDf.
      join(
        airTrafficData,
        airTrafficData.col("faflightid") === maxProgressDf.col("flightid_max_progress")).
      dropDuplicates("faflightid")

    // time representing columns
    val timeColumns = airTrafficData.schema.fields.
      filter(column => column.dataType == org.apache.spark.sql.types.TimestampType)

    // create single field from date and drop column
    timeColumns.foreach(field => {
      val fieldName = field.name

      airTrafficData = airTrafficData.
        withColumn("year_" + fieldName, functions.year(airTrafficData.col(fieldName))).
        withColumn("month_" + fieldName, functions.month(airTrafficData.col(fieldName))).
        withColumn("day_" + fieldName, functions.dayofmonth(airTrafficData.col(fieldName))).
        withColumn("hour_" + fieldName, functions.hour(airTrafficData.col(fieldName)))

      airTrafficData = airTrafficData.drop(fieldName)
    })

    // create and save StringIndexers for categorical features mapping
    categoricalVariables.foreach(catVariable => {
      val catVariableIndexer = new StringIndexer().
        setInputCol(catVariable).
        setOutputCol(catVariable + "(indexed)").
        setHandleInvalid("skip").
        fit(airTrafficData)

      catVariableIndexer.write.overwrite().
        save("hdfs:///user/root/FlightDelayModels/categoricalFeatureIndexers/" + catVariable)
    })

    // list containing columns not to consider
      val columnsToDrop = ListBuffer("max_progress", "progress",
        "blocked", "cancelled", "status",
        "destination_name", "destination_iata", "origin_iata", "origin_name",
        "filed_airspeed", "filed_ete", "filed_distance", "tailnumber")

    // save on Cassandra table "historyCleaned"
    airTrafficData.drop(columnsToDrop:_*).write.format("org.apache.spark.sql.cassandra").
       options(Map(
         "table" -> "cleaned",
         "keyspace" -> "flight_aware" ,
         "cluster" -> "Test Cluster",
         "confirm.truncate" -> "true")).
       mode("Overwrite").
       save()
  }
}
