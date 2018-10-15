package neu.pdpmr.project
import java.io.{File, FileWriter}
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.regression.GBTRegressionModel
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import scala.sys.process._
/*
 * @author Yang Xia, Jiangtao Xu, Yu Wen
 */
object Model {
    def main(args: Array[String]): Unit ={

      //val tempDirPath: Option[String] = util.Properties.envOrNone("TEMP_DIR_PATH")
      //var temPath = "temp"
      //if (!tempDirPath.isEmpty) temPath = tempDirPath.get
      var temPath =
      if (args.length>0) temPath = args(0)

      val modeljarpath = getClass.getResource("/pretrained_model.zip")
      val modelfsPath = new File(temPath + "/pretrained_model.zip")
      FileUtils.copyURLToFile(modeljarpath, modelfsPath)
      Process("unzip -n "+ temPath +"/pretrained_model.zip -d " + temPath).!!
      Process("rm "+ temPath + "/pretrained_model.zip ").!

      val datasetjarpath = getClass.getResource("/dataset_820k.csv")
      val datasetfsPath = new File(temPath + "/dataset_820k.csv")
      FileUtils.copyURLToFile(datasetjarpath, datasetfsPath)



      val queryFile = temPath + "/query.csv"
      val pw = new FileWriter(queryFile)

      for (line <- scala.io.Source.stdin.getLines){
        pw.append(line + "\n")
      }

      pw.close()


      val conf = new SparkConf().setMaster("local[*]")setAppName("Predicting Download")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      val modelPath = "pretrained_model"
      var dataset = DataReader.getPredictionDataFrame(queryFile,temPath + "/dataset_820k.csv",sc,sqlContext)

      val trained_model = GBTRegressionModel.load(modelPath)

      val predictions = trained_model.transform(dataset)



      val result = predictions.select("prediction").collect().toList.map(row => row.getDouble(0))
      for (elem <- result) {
        println(elem)
      }

      val evaluator = new RegressionEvaluator()
              .setLabelCol("label")
              .setPredictionCol("prediction")
              .setMetricName("rmse")
      val rmse = evaluator.evaluate(predictions)
      println("Dataset size "+dataset.count())
      println("Root Mean Squared Error (RMSE) on test data = " + rmse)



//      val name = "output_all.csv"
//      val fw = new FileWriter(name)
//      for (i <- result) {
//        fw.append(i.toString().substring(1,i.toString().length - 1 ))
//        fw.append("\n")
//      }
//      fw.close()


    }



}
