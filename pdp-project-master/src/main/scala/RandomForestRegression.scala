import neu.pdpmr.project.DataReader
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}

/*
 * @author Yang Xia, Jiangtao Xu, Yu Wen
 */
object RandomForestRegression {
  def main(args: Array[String]): Unit = {
    val inputDir = args(0)
    val outputDir = args(1)
    val mode = args(2)
    val csvName = args(3)

    // get parameters for GBT
    val numTree = 100
    val maxDepth = 15

    //specify model names according to algorithm, csvName and its parameters
    val modelName = "RandomForest-offJam-offTaste" +csvName +"-numTree"+numTree+"-maxDepth"+maxDepth


    val conf = new SparkConf().setAppName(modelName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val dataset = DataReader.getTrainingDataFrame(csvName,inputDir,sqlContext)
    val Array(train,test) = dataset.randomSplit(Array(0.99,0.01))


    if(mode == "training") {
      println("Start training models.....")
      val featureStra ="auto"
      val rf = new RandomForestRegressor()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setMaxDepth(maxDepth)
        .setFeatureSubsetStrategy(featureStra)
        .setNumTrees(numTree)
      val model = rf.fit(train)
      model.save(outputDir+"/"+modelName)
    }

    println("Loading pretrianed model")
    val trained_model = RandomForestRegressionModel.load(outputDir+"/"+modelName)

    println("Making predictions")
    val predictions = trained_model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)

    println("Testset size "+test.count())
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)


  }

}
