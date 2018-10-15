import neu.pdpmr.project.DataReader
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
/*
 * @author Yang Xia, Jiangtao Xu, Yu Wen
 */
object LinearRegression {

  def main(args: Array[String]): Unit = {
    val inputDir = args(0)
    val outputDir = args(1)
    val mode = args(2)
    val csvName = args(3)

    //specify model names according to algorithm
    val modelName = "LinearRegression-offJam-offTaste" + csvName

    val conf = new SparkConf().setAppName(modelName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val dataset = DataReader.getTrainingDataFrame(csvName, args(0), sqlContext)
    val Array(train, test) = dataset.randomSplit(Array(0.99, 0.01))

    if (mode == "training") {
      println("Start training models.....")
      val lr = new LinearRegression()
      val model = lr.fit(train)
      model.save(outputDir + "/" + modelName)
    }

    println("Loading pre-trained model")
    val trained_model = LinearRegressionModel.load(outputDir + "/" + modelName)

    println("Making predictions")
    val predictions = trained_model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)

    println("Dataset size " + test.count())
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)

  }


}
