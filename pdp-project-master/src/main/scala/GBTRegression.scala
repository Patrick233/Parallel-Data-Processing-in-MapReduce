import neu.pdpmr.project.DataReader
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/*
 * @author Yang Xia, Jiangtao Xu, Yu Wen
 */
object GBTRegression{

  def main(args: Array[String]): Unit = {
    val inputDir = args(0)
    val outputDir = args(1)
    val mode = args(2)
    val csvName = args(3)

    // get parameters for GBT
    val maxIter = 10
    val maxDepth = 10

    //specify model names according to algorithm, csvName and its parameters
    val modelName = "GBTRegression-offJam-offTaste" +csvName +"-maxIter"+maxIter+"-maxDepth"+maxDepth


    val conf = new SparkConf().setAppName(modelName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val dataset = DataReader.getTrainingDataFrame(csvName,inputDir,sqlContext)
    val Array(train,test) = dataset.randomSplit(Array(0.99,0.01))


    if(mode == "training") {
      println("Start training models.....")
          val gbt = new GBTRegressor()
            .setLabelCol("label")
            .setFeaturesCol("features")
            .setMaxIter(maxIter)
            .setMaxDepth(maxDepth)
          val model = gbt.fit(train)
          model.save(outputDir+"/"+modelName)
    }

    println("Loading pre-trained model")
    val trained_model = GBTRegressionModel.load(outputDir + "/" + modelName)


    println("Making predictions")
    val predictions = trained_model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)

    println("testSet size "+test.count())
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)

  }


}
