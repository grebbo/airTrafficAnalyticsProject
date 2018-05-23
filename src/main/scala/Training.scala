import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexerModel, VectorAssembler}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession

object Training {
  def main(args: Array[String]): Unit = {

    // create SparkSession object with Cassandra connector host
    val spark = SparkSession.builder().
      master("yarn").
      appName("AirTrafficAnalytics").
      config("spark.cassandra.connection.host", "slave-1.localdomain").
      getOrCreate()
    import spark.implicits._

    // load stringIndexers(already fitted)
    val stringIndexers: Array[StringIndexerModel] = FileSystem.get(new Configuration()).
      listStatus(new Path("hdfs:///user/root/FlightDelayModels/categoricalFeatureIndexers/")).
      map(filePath => StringIndexerModel.load(filePath.getPath.toString))

    // import cleaned data
    val airTrafficDataPreProcessed = spark.read.
      format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "cleaned", "keyspace" -> "flight_aware")).
      load()

    // name of the label column
    val labelColumn = "arrival_delay"

    // object assembler
    val vectorAssembler = new VectorAssembler().
      setInputCols(airTrafficDataPreProcessed.columns.
        filterNot(_.equals(labelColumn))).
      setOutputCol("features")

    // rndForestRegressor
    val randomForestRegressor = new RandomForestRegressor().
      setLabelCol(labelColumn).
      setFeaturesCol("features").
      setPredictionCol("predicted_" + labelColumn)

    // object xValidator with parameters upon iterate
    val crossValidator = new CrossValidator().
      setEstimator(randomForestRegressor).
      setEstimatorParamMaps(
        new ParamGridBuilder().
          addGrid(randomForestRegressor.numTrees, Array(4, 8, 20)).
          addGrid(randomForestRegressor.maxDepth, Array(4, 10, 20)).
          build()).
      setEvaluator(
        new RegressionEvaluator().
          setLabelCol(labelColumn).
          setPredictionCol("predicted_" + labelColumn).
          setMetricName("rmse")).
      setNumFolds(3)

    // define the pipeline for execution order
    val trainingPipeline = new Pipeline().
      setStages(stringIndexers ++ Array(vectorAssembler, crossValidator))

    // training phase and save the model
    trainingPipeline.fit(airTrafficDataPreProcessed).
      write.overwrite().
      save("hdfs:///user/root/FlightDelayModels/predictionModels/randomForestRegressorModel")
  }
}
