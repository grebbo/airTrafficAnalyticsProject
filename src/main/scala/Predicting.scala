import java.io.IOException
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector._

object Predicting {
  def main(args: Array[String]): Unit = {

    // create SparkSession object with Cassandra connector host
    val spark = SparkSession.builder().
      master("yarn").
      appName("AirTrafficAnalytics").
      config("spark.cassandra.connection.host", "slave-1.localdomain").
      getOrCreate()
    import spark.implicits._

    // input data as csv
    val predictionData = spark.read.
      option("header", "true").
      option("inferSchema", "true").
      csv(args(0))

    val predictionModel = PipelineModel.
      load("hdfs:///user/root/FlightDelayModels/predictionModels/randomForestRegressorModel")

    val prediction = predictionModel.transform(predictionData)

    // saving results to output table
    try {
      // TODO: overwrite or append ?
      prediction.write.format("org.apache.spark.sql.cassandra").
        options(Map(
          "table" -> "prediction",
          "keyspace" -> "flight_aware" ,
          "cluster" -> "Test Cluster",
          "confirm.truncate" -> "true")).
        mode("Overwrite").
        save()
    } catch {
      case IOException => {
        prediction.createCassandraTable("flight_aware", "prediction")
        prediction.write.format("org.apache.spark.sql.cassandra").
          options(Map(
            "table" -> "prediction",
            "keyspace" -> "flight_aware" ,
            "cluster" -> "Test Cluster",
            "confirm.truncate" -> "true")).
          mode("Overwrite").
          save()
      }
    }
  }
}
