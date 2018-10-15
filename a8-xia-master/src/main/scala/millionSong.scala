/**
  * Created by Patrick on 2017/11/13.
  */

import java.io.FileWriter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

object millionSong {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Million Music")
    val sc = new SparkContext(conf)

    val input = sc.textFile("input/MillionSongSubset/song_info.csv")
    val songInfo = input.mapPartitionsWithIndex { (idx, iterate) => if (idx == 0) iterate.drop(1) else iterate }

    val genreInput = sc.textFile("input/MillionSongSubset/artist_terms.csv")

    val records = songInfo.map(row => new SongInfo(row)).persist()

    val headerAndRows = genreInput.map(line => line.split(";").map(_.trim))
    val header = headerAndRows.first
    val artistRecords = headerAndRows.filter(_(0) != header(0))

    getDistinctSongs(records)
    getDistinctArtists(records)
    getDistinctAlbum(records)
    getTop5(records, "loudness")
    getTop5(records, "length")
    getTop5(records, "tempo")
    getTop5FamiliarArtist(records)
    getTop5HottestSongs(records)
    getTop5HottestArtist(records)
    getTop5PopularKeys(records)
    getTop5ProlificArtist(records)
    getTop5HottestGenre(records,artistRecords)
    getCommonWords(records)

  }

  /**
    * The getDistinctSongs function gets the number of distinct track-id's in the dataset.
    */
  private def getDistinctSongs(x: RDD[SongInfo]) = {
    val numberOfSongs = x.map(_.getSongId()).distinct().count()
    println("Number of Songs is: ", numberOfSongs)
  }

  /**
    * The getDistinctArtists function gets the number of distinct artist-id's in the dataset.
    */
  private def getDistinctArtists(x: RDD[SongInfo]) = {
    val numberOfArtists = x.map(_.getArtId()).distinct().count()
    println("Number of Artists is: ", numberOfArtists)
  }

  /**
    * The getDistinctAlbum function gets the number of distinct albums in the dataset.
    */
  private def getDistinctAlbum(x: RDD[SongInfo]) = {
    val artistAlbumTuple = x.map(line => (line.getArtId(), line.getAlbum())).distinct()
    val numbleOfAlbum = artistAlbumTuple.countByKey().foldLeft(0l)(_ + _._2)
    println("Number of Albums is: ", numbleOfAlbum)
  }

  /**
    * The getTop5 function gets the top 5 loudest songs from the dataset by sorting (TrackId,Loudness) on loudness
    * and picks the top 5 from the list.
    */
  private def getTop5(x: RDD[SongInfo], metric: String) = {
    val songLoudnessTuple = x.map(line => ((line.getSongId(), line.getTitle()), line.getMetric(metric))).distinct()
    val top5LoudestSongs = songLoudnessTuple.sortBy(_._2, false).top(5).toList
    makeCSV(top5LoudestSongs, "output/top5" + metric)
  }


  /**
    * The getTop5FamiliarArtist function gets the top 5 familiar songs from the dataset by sorting (ArtistId,Familiarity) on Familiarity
    * and picks the top 5 from the list.
    */
  private def getTop5FamiliarArtist(x: RDD[SongInfo]) = {
    val artistFamiliarityTuple = x.map(line => ((line.getArtId(),line.getArtistName()), line.getArtFam())).distinct()
    val top5FamiliarArtist = artistFamiliarityTuple.sortBy(_._2, false).top(5).toList
    makeCSV(top5FamiliarArtist, "output/top5FamiliarArtist")
  }

  /**
    * The getTop5PopularKeys function gets the top 5 popular keys from the dataset by doing counByKey on Keys and sort on Key_Confidence
    * and picks the top 5 from the list.
    */
  private def getTop5PopularKeys(x: RDD[SongInfo]) = {
    val keyCountTuple = x.filter(_.getKeyConf() > 0.7).map(line => (line.getKey(), 1))
    val top5PopularKey = keyCountTuple.countByKey().toSeq.sortWith(_._2 > _._2).take(5).toList
    makeCSV(top5PopularKey, "output/top5PopularKeys")
  }

  /**
    * The getTop5ProlificArtist function gets the top 5 prolific artists from the data set by doing countByKey on ArtistId and sort on TrackId's
    * to get the most number of tracks for each artist and thus pick the top 5 from the list.
    */
  private def getTop5ProlificArtist(x: RDD[SongInfo]) = {
    val artistSongTuple = x.filter(line => !line.getArtId().isEmpty && !line.getSongId().isEmpty).map(line => ((line.getArtId(),line.getArtistName()), line.getSongId())).distinct()
    val top5ProlificArtists = artistSongTuple.countByKey().toSeq.sortWith(_._2 > _._2).take(5).toList
    makeCSV(top5ProlificArtists, "output/top5ProlificArtist")
  }

  /**
    * The getTop5HottestArtist function gets the top 5 hottest artists from the dataset by doing sort on artist_hotness
    * and picks the top 5 from the list.
    */
  private def getTop5HottestArtist(x: RDD[SongInfo]) = {
    val artistHotnessTuple = x.filter(line => !line.getArtId().isEmpty).map(line => ((line.getArtId(),line.getArtistName()), line.getArtHot())).distinct().persist()
    val top5HottestArtist = artistHotnessTuple.sortBy(_._2, false).top(5).toList
    makeCSV(top5HottestArtist, "output/top5HottestArtist")
  }

  /**
    * The getTop5HottestSongs function gets the top 5 hottest songs from the dataset by grouping on
    * (TrackId,SongHotness) and doing sort on songs_hotness thus picks the top 5 from the list.
    */
  private def getTop5HottestSongs(x: RDD[SongInfo]) = {
    val songHotnessTuple = x.filter(line => !line.getSongId().isEmpty && Try(line.getSongHot().toFloat).isSuccess).map(line => ((line.getSongId(),line.getTitle()), line.getSongHot())).distinct()
    val top5HottestSongs = songHotnessTuple.sortBy(_._2, false).top(5).toList
    makeCSV(top5HottestSongs, "output/top5HottestSongs")
  }

  /**
    * The getTop5HottestGenre function gets the top 5 hottest genre from the dataset by joining
    * (ArtistId, ArtistTerm) and (ArtistId,ArtistHotness) and does a combineByKey to get the mean artist_hotness
    * and sorts on the mean to pick the top 5 genre.
    */
  def getTop5HottestGenre(records: RDD[SongInfo], artistRecords: RDD[Array[String]]): Unit = {

    val artist_term = artistRecords.map(lines => (lines(0), lines(1))).distinct
    val artist_hotness = records.map(lines => (lines.getArtId(), lines.getArtHot()))
    var afterJoin = artist_term.join(artist_hotness)
    var afterGrouping = afterJoin.map(gr => (gr._2._1, gr._2._2)).groupBy(_._1)
    val top5Genres = afterGrouping.map(lines => (lines._1, getMeanScore(lines._2))).sortBy(-_._2).take(5).toList
    makeCSV(top5Genres,"output/top5genres")
  }

  /**
    * The getCommonWords function gets the top 5 common words from the dataset by counting the frquency of each
    * word in Song_Tile column and thus picking the top 5 from the list.
    */
  private def getCommonWords(x: RDD[SongInfo]) = {
    val ignored = Set(
      "THAT", "WITH", "THE", "AND", "TO", "OF",
      "A", "IT", "SHE", "HE", "YOU", "IN", "I",
      "HER", "AT", "AS", "ON", "THIS", "FOR",
      "BUT", "NOT", "OR")
    val words = x.flatMap(line => line.getTitle().filter(c => c.isLetter || c.isWhitespace).toUpperCase().split(" ")).filter {
      !_.isEmpty
    }
    val wordsCount = words.filter {
      !ignored.contains(_)
    }.map(w => (w, 1))
    val top5Words = wordsCount.countByKey().toSeq.sortWith(_._2 > _._2).take(5).toList
    makeCSV(top5Words, "output/top5CommonWords")
  }

  def getMeanScore(values : Iterable[(String, Double)]) : Double = {
    var noOfEntries : Int = 0
    var sum : Double = 0.00
    values.foreach(av => {
      noOfEntries += 1
      sum += av._2
    })
    sum/noOfEntries
  }

  def makeCSV(thingsToPrint: List[(Any, Any)], fileName: String): Unit = {
    val name = fileName
    val fw = new FileWriter(name)
    for (i <- thingsToPrint) {
      fw.append(i._1.toString)
      fw.append(",")
      fw.append(i._2.toString)
      fw.append("\n")
    }
    fw.close()
  }

}
