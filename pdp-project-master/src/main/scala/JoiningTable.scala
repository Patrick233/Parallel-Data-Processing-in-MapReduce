import java.io.FileWriter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Yang Xia on 2017/11/27.
  */
object JoiningTable {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Million Music")
    val sc = new SparkContext(conf)
    val spark_session: SparkSession = SparkSession.builder.master("local").getOrCreate

    val downloadInput = sc.textFile("downloads.csv")
    val downloads = downloadInput.mapPartitionsWithIndex { (idx, iterate) => if (idx == 0) iterate.drop(1) else iterate }
    val downloadRecord = downloads.map(row => new DownloadInfo(row)).filter(_.checkValidity()).map(d => (d.getArtist(), d.getTitle(), d.getMeanPrice(), d.getDownload(), d.getConfidence()))
    val downloadDF = spark_session.createDataFrame(downloadRecord).toDF("DownloadArtistName", "DownloadTitle", "Price", "Download", "Confidence") // get download data frame

    val songInput = sc.textFile("mergedSongInfo")
    val songRecord = songInput.map(row => new MergedSongInfo(row))
    val songInfos = songRecord.map(
      s => (s.getArtistName(), s.getTitle(), s.getTrackId(), s.getSongId(), s.getArtFam(), s.getArtHot(), s.getDuration(), s.getLoudness(), s.getSongHot(), s.getTempo()))
    val songInfoDF = spark_session.createDataFrame(songInfos).toDF("ArtistName", "Title", "TrackId", "SongId", "ArtFam", "ArtHot", "Duration", "Loudness", "SongHot", "Tempo") // get song info data frame


    val download_songInfo_DF = songInfoDF.join(downloadDF, downloadDF("DownloadArtistName") === songInfoDF("ArtistName") && downloadDF("DownloadTitle") === songInfoDF("Title")
      , "cross").drop(downloadDF("DownloadArtistName")).drop(downloadDF("DownloadTitle"))


    val rows: RDD[Row] = download_songInfo_DF.rdd


    System.out.println("download_songInfo_DF " + rows.count())


    val jamInput = sc.textFile("jam_to_msd.tsv")
    val jams = jamInput.mapPartitionsWithIndex { (idx, iterate) => if (idx == 0) iterate.drop(1) else iterate }.map(new Jam(_))
    val jamCount = jams.map(j => (j.getTrack(), j.getJam())).countByKey()
    val jam_rdd = sc.parallelize(jamCount.toSeq)
    // convert Collection[Map] to RDD
    val jamDF = spark_session.createDataFrame(jam_rdd).toDF("JamTrackId", "JamCount") // get jam count data frame

    val jam_download_song_DF = download_songInfo_DF.join(jamDF, download_songInfo_DF("TrackId") === jamDF("JamTrackId"), "left_outer").drop(jamDF("JamTrackId")).na.fill(0, Seq("JamCount"))

    val tasteInput = sc.textFile("train_triplets.txt")
    val taste = tasteInput.mapPartitionsWithIndex { (idx, iterate) => if (idx == 0) iterate.drop(1) else iterate }.map(new Taste(_))
    val taste_keyPair = taste.map(t => (t.getSong(), t.getCount())).reduceByKey(_ + _)
    val tasteDF = spark_session.createDataFrame(taste_keyPair).toDF("SongId", "TasteCount") // get taste count data frame

    val taste_jam_download_song_DF = jam_download_song_DF.join(tasteDF, jam_download_song_DF("SongId") === tasteDF("SongId"), "left_outer").drop(tasteDF("SongId")).na.fill(0, Seq("TasteCount"))

    val result = taste_jam_download_song_DF.collect().toList

    val name = "filledSongInfoDataset"
    val fw = new FileWriter(name)
    for (i <- result) {
      fw.append(i.toString().substring(1, i.toString().length - 1))
      fw.append("\n")
    }
    fw.close()

  }

}



