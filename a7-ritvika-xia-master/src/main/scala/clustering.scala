import java.io.{BufferedWriter, File, FileWriter}

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

import scala.collection.mutable
import scala.util.control.Breaks
import scala.collection.mutable.ListBuffer
import scala.util.{Random, Try}

/**
  * Created by Patrizio and Ritvika on 2017/10/29.
  */
object clustering {

  val log = LogManager.getLogger("SongAnalysis")
  log.setLevel(Level.INFO)

  def main(args: Array[String]) {

//    val inputPath = args(0)
//    val K = args(1).toInt

    val K = 3
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

//    val input = sc.textFile(inputPath+"/song_info.csv")

    val input = sc.textFile("input/MillionSongSubset/song_info.csv")
    //val input = sc.textFile("input/all/song_info.csv")

    val time = new FileWriter("out/Time_counter")
    val start = System.currentTimeMillis()

    val lines = input.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    var records = lines.map(row => new SongInfo(row))

    // Sanity test
    records = records.filter(_.checkValidity()).persist()

    //fuzzy loudness
//    val loudnessHAC = hacGetCentroid(records, List("loudness"))
    val loudness_metric = List("loudness")
    var jobStart = System.currentTimeMillis()
    val loudness = kMeans(records, loudness_metric , K).toArray
    var jobEnd = System.currentTimeMillis()
    writeTimeToFile(loudness_metric.head, (jobEnd-jobStart).toString, time)
    val clusters_loudness = getCluster(records, loudness,loudness_metric)
    saveToFile(clusters_loudness ,loudness_metric)
    printResult(loudness)

    val hacLoudness = hacGetCentroid(records,loudness_metric,3)

    //fuzzy length
    val length_metric = List("length")
    jobStart = System.currentTimeMillis()
    val length = kMeans(records, length_metric, K).toArray
    jobEnd = System.currentTimeMillis()
    writeTimeToFile(length_metric.head, (jobEnd-jobStart).toString, time)
    val clusters_length = getCluster(records, length,length_metric)
    saveToFile(clusters_length ,length_metric)
    printResult(length)

    //fuzzy tempo
    val tempo_metric = List("tempo")
    jobStart = System.currentTimeMillis()
    val tempo = kMeans(records, tempo_metric, K).toArray
    jobEnd = System.currentTimeMillis()
    writeTimeToFile(tempo_metric.head, (jobEnd-jobStart).toString, time)
    val clusters_tempo = getCluster(records, tempo,tempo_metric)
    saveToFile(clusters_tempo ,tempo_metric)

    printResult(tempo)

    //fuzzy hotness
    val hotness_metric = List("hotness")
    jobStart = System.currentTimeMillis()
    val hotness = kMeans(records, hotness_metric, K).toArray
    jobEnd = System.currentTimeMillis()
    writeTimeToFile(hotness_metric.head, (jobEnd-jobStart).toString, time)
    val clusters_hotness = getCluster(records, hotness,hotness_metric)
    saveToFile(clusters_hotness,hotness_metric)

    printResult(hotness)

    //fuzzy combined hotness using artist_hotness, and song_hotness
    val combinesHotness_metric = List("artist_hotness", "hotness")
    jobStart = System.currentTimeMillis()
    val combinedHotness = kMeans(records, combinesHotness_metric, K).toArray
    jobEnd = System.currentTimeMillis()
    writeTimeToFile("combinedHotness", (jobEnd-jobStart).toString, time)
    val clusters_combinedHotness = getCluster(records, combinedHotness,combinesHotness_metric)
    saveToFile(clusters_combinedHotness ,combinesHotness_metric)

    printResult(combinedHotness)
    time.close()


    //SubProblem 2
    val similarArtist = sc.textFile("input/MillionSongSubset/similar_artists.csv")
//    val similarArtist = sc.textFile(inputPath+"/similar_artists.csv")
    val l2 = similarArtist.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val countSimilar = l2.map(_.split(";")).map(x => (x(0), x(1))).groupByKey().map(x => (x._1, x._2.size))
    val artistSongs = ArtistProductXFamiliarity(records)
    val popularity = countSimilar.join(artistSongs).map(x => (x._1, x._2._1 * x._2._2))
    val trendsetters = popularity.sortBy(_._2).take(30)

//    val artistTerm = sc.textFile(inputPath+"/artist_terms.csv")

    val artistTerm = sc.textFile("input/MillionSongSubset/artist_terms.csv")
    val l3 = artistTerm.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val termMap = l3.map(_.split(";")).map(x => (x(0), x(1))).groupByKey().collect().toList
    //val termMap = test.toList
    val commonalityMap = new mutable.HashMap[String, Int]()
    for (artist1 <- termMap) {
      for (artist2 <- termMap) {
        if (artist1._1 != artist2._1) {
          val key = artist1._1 + ";" + artist2._1
          val commonality = countCommonality(artist1._2, artist2._2)
          commonalityMap.put(key, commonality)
        }
      }
    }
    val cMap = sc.parallelize(commonalityMap.toSeq)
  }

  def countCommonality(termsList1: Iterable[String], termsList2: Iterable[String]): Int = {
    var count = 0
    for (term1 <- termsList1) {
      for (term2 <- termsList2) {
        if (term1 == term2) {
          count += 1
        }
      }
    }
    count


  }

  //perform kmeans clustering 10 times.
  def kMeans(x: rdd.RDD[SongInfo], metrics: List[String], K: Int): Seq[(Double, Double)] = {

    val points = x.map(_.getMetric(metrics))
    val centroid = points.takeSample(withReplacement = false, K, System.nanoTime.toInt)

    var newcentroid = new Array[(Double, Double)](K)
    var iter = 0
    while (iter < 10) {
      newcentroid = newCentroid(x, centroid, metrics)
      iter += 1
    }
    newcentroid
  }

  //we find the nearest centroid to a given point
  def findNearest(point: (Double, Double), centroids: Seq[(Double, Double)]) = {
    centroids.reduceLeft((a, b) => if (euclideanDist(point, a) < euclideanDist(point, b)) a else b)
  }

  def euclideanDist(p1: (Double, Double), p2: (Double, Double)): Double = {
    Math.sqrt(Math.pow(p1._1 - p2._1, 2.00) + Math.pow(p1._2 - p2._2, 2.00))
  }


  def newCentroid(points: RDD[SongInfo], centroids: Seq[(Double, Double)], metrics: List[String])
  : Array[(Double, Double)] = {

    val newClusters = points.map(x => (x.getSongId(), x.getMetric(metrics)))
      .groupBy(x => findNearest(x._2, centroids)).map(line => getMeanScore(line._2)).collect().toSeq
    newClusters.toArray
  }

  def getMeanScore(values: Iterable[(String, (Double, Double))]): (Double, Double) = {
    var noOfEntries: Int = 0
    var xSum: Double = 0.00
    var ySum: Double = 0.00
    values.foreach(av => {
      noOfEntries += 1
      xSum += av._2._1
      ySum += av._2._2
    })
    (xSum / noOfEntries, ySum / noOfEntries)
  }

  def getCluster(x: RDD[SongInfo], centroid: Seq[(Double, Double)], metric: List[String]): Map[
    (Double, Double), Iterable[(String,String, (Double, Double))]] = {
    val result = x.map(x => (x.getSongId(),x.getTitle(), x.getMetric(metric))).groupBy(x => findNearest(x._3, centroid)).collect().toMap
    result
  }

  def hacGetCentroid(x:RDD[SongInfo], metric:List[String], K:Int): Array[(Double,Double)] = {

    val rdd1 = x.map(x => (x.getSongId(),x.getMetric(metric)._1)).sortBy(_._2).zipWithIndex().map(x=> (x._2,x._1))
    val rdd2 = x.map(x => (x.getSongId(),x.getMetric(metric)._1)).sortBy(_._2).zipWithIndex().map(x=> (x._2+1,x._1))

    val rddx = rdd1.join(rdd2).map(x => (x._1,x._2._2._2,x._2._1._2)).sortBy(_._1).collect().toVector

    while(rddx.size>3){
      for(i <- 1 until(rddx.size-1)){
        if(rddx(i+1)._3-rddx(i)._2>rddx(i)._3-rddx(i-1)._2){
        }

      }

    }

    val res = new Array[(Double,Double)](2)
    var count =0
    for(x <- rddx){
      res(count) = (x._3,0.0)
      count +=1
    }
    res
  }

  /*
  def hacGetCentroid(x: RDD[SongInfo], metric: List[String]): Seq[(Double,Double)] = {
    val withIndex = x.map(_.getMetric(metric)).zipWithIndex()
    val points = withIndex.map { case (a, b) => (b, a) }
    var index = 0
    var count = 0
    var nearestpoint = (0.0,0.0)
    val result = new Array[(Double,Double)](3)
    while (true) {
      if (!Try(points.lookup(index + 1)).isSuccess) return result
      val point = points.lookup(index)
      if (index == 0) {
        nearestpoint = point(2)
        result(count) = nearestpoint
        count += 1
      }
      if (euclideanDist(points.lookup(index + 1)(2),point(2)) < euclideanDist(point(2),
        nearestpoint)) {
        result(count) = nearestpoint
        nearestpoint = points.lookup(index + 1)(2)
        count += 1
      }
      else nearestpoint = point(2)
      if (count > 2) return result
      index += 1
    }
    result
  }
  */

  def ArtistProductXFamiliarity(records: rdd.RDD[SongInfo]): RDD[(String, Double)] = {
    val artistuple = records.map { l => (l.getArtId(), l.getArtFam()) }
    artistuple.reduceByKey((x, y) => x + y)
  }


  def printResult(result: Array[(Double, Double)]): Unit = {
    for (i <- 0 until (result.size))
      println(result(i)._1, result(i)._2)
  }

  def saveToFile(outputData: Map[(Double, Double), Iterable[(String,String, (Double, Double))]],
                 metric: List[String]): Unit = {

    var header = new String
    var fname = new String
    if(metric.size==1){
      header = metric.head + "_x" + "," + metric.head+"_y"
      fname = metric.head
    }
    else{
      header = metric.head+"_x"+","+metric.tail.head+"_y"
      fname = metric.head+"_"+metric.tail.head
    }
    val toWrite = "Centroid_x,Centroid_y,Song_Id,Title,"+header+"\n"
    val fw = new FileWriter("out/"+fname+".csv")
    fw.append(toWrite)
//    val data = outputData.map(x => getString(x)).fold("")((x,y) => x + "\n" + y)
//    val data =
    outputData.map(x => getString(x, fw))
//    fw.append(data)
    fw.close()

  }

  def getString(x: ((Double, Double), Iterable[(String, String, (Double, Double))]), fw: FileWriter)
  : String ={
    var result = ""
    x._2.foreach(y => fw.append(x._1._1 +"," + x._1._2 + "," + y._1.toString + "," + y._2.toString + ","
      + y
      ._3._1 + "," + y._3._2 + "\n"))
    result

  }

  def writeTimeToFile(metric : String, str : String, fw : FileWriter) : Unit ={
    fw.append(metric + ","+str+"\n")

  }


}
