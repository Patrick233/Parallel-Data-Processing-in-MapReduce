import java.io.{File, FileWriter, PrintWriter}

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try
/*
 * @author Yang Xia, Jiangtao Xu, Yu Wen
 */
object getCleanedSongInfo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Million Music")
    val sc = new SparkContext(conf)
    val spark_session: SparkSession = SparkSession.builder.master("local").getOrCreate

    val songInput = sc.textFile("all/song_info.csv")
    val songs = songInput.mapPartitionsWithIndex { (idx, iterate) => if (idx == 0) iterate.drop(1) else iterate }
    val songRecord = songs.map(row => new SongInfo(row))//read song Info from files
    val artist_songInfo_keyPair = songRecord.map(s => (s.getArtId(),
      (s.getTempo(),s.getSongHot(),s.getArtHot(),s.getLoudness(),s.getDuration(),s.getArtFam()))).groupByKey()

    val songInfos = songRecord.map(
      s => (s.getArtId(),s.getArtistName(),s.getTitle(),s.getTrackId(), s.getSongId(),s.getArtFam(),s.getArtHot(),s.getDuration(),s.getLoudness(),s.getSongHot(),s.getTempo()))
    val songInfoDF = spark_session.createDataFrame(songInfos).toDF("ArtistId","ArtistName","Title","TrackId", "SongId","ArtFam","ArtHot","Duration","Loudness","SongHot","Tempo")// get song info data frame

    val averageSongInput = sc.textFile("avarage_song_info")
    val averageSongInfo = averageSongInput.map(row => new AverageSongInfo(row)).map(as =>
      (as.getArtistId(),as.getTempo(),as.getSongHot(),as.getArtHot(),as.getLoudness(),as.getDuration(),as.getArtFam()))
    val averageSongInfoDF = spark_session.createDataFrame(averageSongInfo).toDF("avgArtistId","avgTempo","avgSongHot","avgArtHot","avgLoudness","avgDuration","avgArtFam")// get avg song info data frame

    val mergedSongInfoDF = songInfoDF.join(averageSongInfoDF,songInfoDF("ArtistId") === averageSongInfoDF("avgArtistId"),"cross").drop(averageSongInfoDF("avgArtistId"))

    val result = mergedSongInfoDF.collect().toList

    val ouptputName = "mergedSongInfo"
    val fw = new FileWriter(ouptputName)
    for (i <- result) {
      fw.append(i.toString().substring(1,i.toString().length - 1 ))
      fw.append("\n")
    }
    fw.close()



  }

}
