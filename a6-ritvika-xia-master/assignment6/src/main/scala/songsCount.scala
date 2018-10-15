import java.io.{FileWriter, StringWriter}
import java.util

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.Ordering
import scala.util.Try
import scala.util.control.Breaks._

/**
  * Created by Patrizio on 2017/10/24.
  */
object songsCount {

  def main(args: Array[String]): Unit = {

    def TRACK_ID = 0

    def ARTIST_ID = 16

    def ALBUM_RELEASE = 22

    def LOUNDNESS = 6

    def DURATION = 5

    def TEMPO = 7

    def SONG_HOTNESS = 25

    def ARTIST_HOTNESS = 20

    def ARTIST_FAMILIARITY = 19

    def KEY = 8

    def KEY_CONFIDENCE = 9

    def TITLE = 24

    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

    val time = new FileWriter("output/Time counter")

    val start = System.currentTimeMillis()

    val input = sc.textFile("input/MillionSongSubset/song_info.csv")
    //val input = sc.textFile("MillionSongSubset/all/song_info.csv")
    val lines = input.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val songInfo = lines.map(x => x.split(";")).persist()



    var jobStart = System.currentTimeMillis()
    // Number of distinct songs
    val totalSongs = songInfo.filter(record => !record(TRACK_ID).isEmpty).
      map(record => record(TRACK_ID)).distinct().count()
    var jobEnd = System.currentTimeMillis()
    time.append("1")
    time.append(",")
    time.append((jobEnd-jobStart).toString)
    time.append("\n")
    println("Total number of songs is: " + totalSongs)

    jobStart = System.currentTimeMillis()
    // Number of distinct artist
    val totalArtists = songInfo.filter(record => !record(ARTIST_ID).isEmpty).
      map(record => record(ARTIST_ID)).distinct().count()
    jobEnd = System.currentTimeMillis()
    time.append("2")
    time.append(",")
    time.append((jobEnd-jobStart).toString)
    time.append("\n")
    println("Total number of Artists is: " + totalArtists)

    jobStart = System.currentTimeMillis()
    // Number of distinct albums
    val totalAlbums = songInfo.filter(record => !record(ALBUM_RELEASE).isEmpty).
      map(record => record(ALBUM_RELEASE)).distinct().count()
    jobEnd = System.currentTimeMillis()
    time.append("3")
    time.append(",")
    time.append((jobEnd-jobStart).toString)
    time.append("\n")
    println("Total number of albums is: " + totalAlbums)

    jobStart = System.currentTimeMillis()
    // Top5 loudest songs
    val top5Loudest = songInfo.
      filter(record => !record(TRACK_ID).isEmpty && Try(record(LOUNDNESS).toDouble).isSuccess).
      map(record => (record(TRACK_ID), record(LOUNDNESS).toDouble)).sortBy(_._2,false).take(5)
    jobEnd = System.currentTimeMillis()
    time.append("4")
    time.append(",")
    time.append((jobEnd-jobStart).toString)
    time.append("\n")
    makeCSVDouble(top5Loudest.toList,"top5Loudest")

    jobStart = System.currentTimeMillis()
    // Top5 longest songs
    val top5Longest = songInfo.
      filter(record => !record(TRACK_ID).isEmpty && Try(record(DURATION).toDouble).isSuccess).
      map(record => (record(TRACK_ID), record(DURATION).toDouble)).sortBy(_._2,false).take(5)
    jobEnd = System.currentTimeMillis()
    time.append("5")
    time.append(",")
    time.append((jobEnd-jobStart).toString)
    time.append("\n")
    makeCSVDouble(top5Longest.toList,"top5longest")

    jobStart = System.currentTimeMillis()
    // Top5 fastest songs
    val top5Fastest = songInfo.
      filter(record => !record(TRACK_ID).isEmpty && Try(record(TEMPO).toDouble).isSuccess).
      map(record => (record(TRACK_ID), record(TEMPO).toDouble)).sortBy(_._2,false).take(5)
    jobEnd = System.currentTimeMillis()
    time.append("6")
    time.append(",")
    time.append((jobEnd-jobStart).toString)
    time.append("\n")
    makeCSVDouble(top5Fastest.toList,"top5fastest")

    jobStart = System.currentTimeMillis()
    // Top5 hottest songs
    val top5Hottest = songInfo.
      filter(record => !record(TRACK_ID).isEmpty && Try(record(SONG_HOTNESS).toDouble).isSuccess).
      map(record => (record(TRACK_ID), record(SONG_HOTNESS).toDouble)).sortBy(_._2,false).take(5)
    jobEnd = System.currentTimeMillis()
    time.append("7")
    time.append(",")
    time.append((jobEnd-jobStart).toString)
    time.append("\n")
    makeCSVDouble(top5Hottest.toList,"top5hottest")


    val artistHotAndFam = songInfo.filter(x => Try(x(ARTIST_HOTNESS).toDouble).isSuccess &&
      !x(ARTIST_ID).isEmpty &&
      Try(x(ARTIST_FAMILIARITY).toDouble).isSuccess)
      .map(v => (v(ARTIST_ID), (v(ARTIST_FAMILIARITY).toDouble, v(ARTIST_HOTNESS).toDouble)))
      .distinct().persist()

    jobStart = System.currentTimeMillis()
    // Top5 most familiar artists
    val top5Familiar = artistHotAndFam.sortBy(_._2._1,false).take(5)
    jobEnd = System.currentTimeMillis()
    time.append("8")
    time.append(",")
    time.append((jobEnd-jobStart).toString)
    time.append("\n")
    println("Top 5 most familiar artists: " + top5Familiar.toList)

    jobStart = System.currentTimeMillis()
    // Top5 most hottest artists
    val top5HottestArtist = artistHotAndFam.sortBy(_._2._2,false).take(5)
    jobEnd = System.currentTimeMillis()
    time.append("9")
    time.append(",")
    time.append((jobEnd-jobStart).toString)
    time.append("\n")
    println("Top 5 hottest artists: " + top5HottestArtist.toList)

    // Top 5 hottest genres
    jobStart = System.currentTimeMillis()
    val artist_terms = sc.textFile("input/MillionSongSubset/artist_terms.csv")
    //val artist_terms = sc.textFile("MillionSongSubset/all/artist_terms.csv")
    val terms = artist_terms
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter
      }
    val termInfo = terms.map(x => x.split(";")).map(record => (record(0), record(1)))
    val artistHottness = artistHotAndFam.map(record => (record._1, record._2._2)).distinct()
    val termHottness = termInfo.join(artistHottness).map { case (artist, (term, hotness)) => (term, hotness.toDouble) }.groupByKey()
    val top5HottnessTerm = termHottness.map(term => (term._1, term._2.sum/term._2.size)).sortBy(_._2,false).take(5)
    jobEnd = System.currentTimeMillis()
    time.append("10")
    time.append(",")
    time.append((jobEnd-jobStart).toString)
    time.append("\n")
    makeCSVDouble(top5HottnessTerm.toList,"top5Genres")

    //Top 5 most popular keys (must have confidence > 0.7)
    jobStart = System.currentTimeMillis()
    val KeyMap = songInfo.filter(record => !record(KEY).isEmpty &&
      Try(record(KEY).toInt).isSuccess &&
      Try(record(KEY_CONFIDENCE).toDouble).isSuccess &&
      record(KEY_CONFIDENCE).toDouble > 0.7).
      map(record => (record(KEY),1)).groupByKey()
    val top5Key = KeyMap.map(key => (key._1,key._2.sum)).sortBy(_._2,false).take(5)
    jobEnd = System.currentTimeMillis()
    time.append("11")
    time.append(",")
    time.append((jobEnd-jobStart).toString)
    time.append("\n")
    makeCSVInt(top5Key.toList,"top5Key")

    //Top 5 most prolific artists
    jobStart = System.currentTimeMillis()
    val prolificArtist = songInfo.filter(x => !x(ARTIST_ID).isEmpty && !x(TRACK_ID).isEmpty).
      map(v => (v(ARTIST_ID), v(TRACK_ID))).groupByKey().map(x => (x._1, x._2.size)).persist()
    val top5Numbers = prolificArtist.map(x => (x._2,x._1)).groupByKey().sortByKey(false).keys.take(5).toList
    val exEquoTop = prolificArtist.filter(x => top5Numbers.contains(x._2)).collect().toList
    jobEnd = System.currentTimeMillis()
    time.append("12")
    time.append(",")
    time.append((jobEnd-jobStart).toString)
    time.append("\n")
    makeCSVInt(exEquoTop,"prolificArtists")


    //Top 5 most common words in song titles
    val ignored = Set(
      "THAT", "WITH", "THE", "AND", "TO", "OF",
      "A", "IT", "SHE", "HE", "YOU", "IN", "I",
      "HER", "AT", "AS", "ON", "THIS", "FOR",
      "BUT", "NOT", "OR", "-", "&", "/")
    jobStart = System.currentTimeMillis()
    val commonWords = songInfo.flatMap(record => record(TITLE).toUpperCase().split(" ")).
      filter(!_.isEmpty).filter(!ignored.contains(_)).countByValue()
    val top5Common = sc.parallelize(commonWords.toSeq).sortBy(_._2,false).take(100)
    jobEnd = System.currentTimeMillis()
    time.append("13")
    time.append(",")
    time.append((jobEnd-jobStart).toString)
    time.append("\n")

    val end = System.currentTimeMillis()

    time.append("total time")
    time.append(",")
    time.append((end-start).toString)
    time.append("\n")
    time.close()
    makeCSVLong(top5Common.toList,"commonWords")
//    val fw = new FileWriter("output/common words")
//    for(i <- top5Common){
//      fw.append(i._1)
//      fw.append(",")
//      fw.append(i._2.toString)
//      fw.append("\n")
//    }
//    fw.close()
    println("Top 5 most popular words: " + top5Common.toList)

  }

  def makeCSVLong(thingsToPrint:List[(String,Long)],fileName:String):Unit ={
    val  name = "output/"+fileName
    val fw = new FileWriter(name)
    for(i <- thingsToPrint){
      fw.append(i._1)
      fw.append(",")
      fw.append(i._2.toString)
      fw.append("\n")
    }
    fw.close()
  }

  def makeCSVInt(thingsToPrint:List[(String,Int)],fileName:String):Unit ={
    val  name = "output/"+fileName
    val fw = new FileWriter(name)
    for(i <- thingsToPrint){
      fw.append(i._1)
      fw.append(",")
      fw.append(i._2.toString)
      fw.append("\n")
    }
    fw.close()
  }

  def makeCSVDouble(thingsToPrint:List[(String,Double)],fileName:String):Unit ={
    val  name = "output/"+fileName
    val fw = new FileWriter(name)
    for(i <- thingsToPrint){
      fw.append(i._1)
      fw.append(",")
      fw.append(i._2.toString)
      fw.append("\n")
    }
    fw.close()
  }

}
