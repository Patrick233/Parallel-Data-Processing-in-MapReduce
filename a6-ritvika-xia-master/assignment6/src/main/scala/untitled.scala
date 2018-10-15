import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Patrizio on 2017/10/24.
  */
object untitled {

  def main(args: Array[String]): Unit = {

    //val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
    val line = sc.textFile("100.txt.utf-8")
    val count = line.map(x => x.split(" ")).count()
    print(count.toString)

  }
}
