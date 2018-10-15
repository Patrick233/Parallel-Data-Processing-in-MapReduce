import scala.util.Try

//author: Jiangtao
class MergedSongInfo(track: String, ai: String, si: String, du: String,
                     ld: String, sh: String, ah: String, tp: String, af: String,
                     an: String, st: String, avg_du: String, avg_ld: String,
                     avg_sh: String, avg_ah: String, avg_tp: String, avg_af: String) extends Serializable {

  var TRACK_ID: String = track
  var SONG_ID: String = si
  var DURATION: String = du
  var LOUDNESS: String = ld
  var SONG_HOTNESS: String = sh
  var ARTIST_HOT: String = ah
  var TEMPO: String = tp
  var ARTIST_FAM: String = af
  var ARTIST_NAME: String = an
  var SONG_TITLE: String = st

  var ARTIST_ID: String = ai
  var AVG_TEMPO: String = avg_tp
  var AVG_SONG_HOTNESS: String = avg_sh
  var AVG_ARTIST_HOT: String = avg_ah
  var AVG_LOUDNESS: String = avg_ld
  var AVG_DURATION: String = avg_du
  var AVG_ARTIST_FAM: String = avg_af


  def this(line: String) = {
    this("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
    def tokens = line.split(",")

    this.ARTIST_ID = tokens(0)
    this.ARTIST_NAME = tokens(1)
    this.SONG_TITLE = tokens(2)
    this.TRACK_ID = tokens(3)
    this.SONG_ID = tokens(4)
    this.ARTIST_FAM = tokens(5)
    this.ARTIST_HOT = tokens(6)
    this.DURATION = tokens(7)
    this.LOUDNESS = tokens(8)
    this.SONG_HOTNESS = tokens(9)
    this.TEMPO = tokens(10)
    this.AVG_TEMPO = tokens(11)
    this.AVG_SONG_HOTNESS = tokens(12)
    this.AVG_ARTIST_HOT = tokens(13)
    this.AVG_LOUDNESS = tokens(14)
    this.AVG_DURATION = tokens(15)
    this.AVG_ARTIST_FAM = tokens(16)


  }


  def getTrackId(): String = this.TRACK_ID


  def getDuration(): String = {
    if (Try(this.DURATION.toDouble).isSuccess) return this.DURATION
    else return this.AVG_DURATION
  }

  def getLoudness(): String = {
    if (Try(this.LOUDNESS.toDouble).isSuccess) return this.LOUDNESS
    else return this.AVG_LOUDNESS
  }

  def getTempo(): String = {
    if (Try(this.TEMPO.toDouble).isSuccess) return this.TEMPO
    else return this.AVG_TEMPO
  }

  def getArtFam(): String = {
    if (Try(this.ARTIST_FAM.toDouble).isSuccess) return this.ARTIST_FAM
    else return this.AVG_ARTIST_FAM
  }

  def getArtHot(): String = {
    if (Try(this.ARTIST_HOT.toDouble).isSuccess) return this.ARTIST_HOT
    else return this.AVG_ARTIST_HOT
  }

  def getSongHot(): String = {
    if (Try(this.SONG_HOTNESS.toDouble).isSuccess) return this.SONG_HOTNESS
    else return this.AVG_SONG_HOTNESS
  }

  def getArtId(): String = this.ARTIST_ID

  def getArtistName(): String = this.ARTIST_NAME.toLowerCase.replaceAll("\\s","").replace(",","").replace(";","")


  def getSongId(): String = this.SONG_ID

  def getTitle(): String = this.SONG_TITLE.toLowerCase.replaceAll("\\s","").replace(",","").replace(";","")


}

