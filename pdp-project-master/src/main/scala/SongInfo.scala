import scala.util.Try

//author: Jiangtao
class SongInfo(track : String, ai : String, si : String, du : String, ld : String, sh : String, ah : String, tp: String, af: String, an: String, st: String) extends Serializable{
  var TRACK_ID : String = track
  var ARTIST_ID : String = ai
  var SONG_ID : String = si
  var DURATION : String = du
  var LOUDNESS : String = ld
  var SONG_HOTNESS : String = sh
  var ARTIST_HOT : String = ah
  var TEMPO : String = tp
  var ARTIST_FAM : String = af
  var ARTIST_NAME : String = an
  var SONG_TITLE : String = st



  def this(line : String) ={
    this("","","","","","","","","","","")
    def tokens = line.split(";")
    this.TRACK_ID = tokens(0)
    this.ARTIST_ID = tokens(16)
    this.SONG_ID = tokens(23)
    this.DURATION = tokens(5)
    this.LOUDNESS = tokens(6)
    this.SONG_HOTNESS = tokens(25)
    this.ARTIST_HOT = tokens(20)
    this.TEMPO = tokens(7)
    this.ARTIST_FAM = tokens(19)
    this.ARTIST_NAME = tokens(17)
    this.SONG_TITLE = tokens(24)

  }



  def getTrackId(): String = this.TRACK_ID


  def getDuration(): String = this.DURATION

  def getLoudness(): String = this.LOUDNESS

  def getTempo(): String = this.TEMPO

  def getArtFam(): String = this.ARTIST_FAM

  def getArtHot(): String = this.ARTIST_HOT

  def getSongHot(): String = this.SONG_HOTNESS

  def getArtId(): String = this.ARTIST_ID

  def getArtistName(): String = this.ARTIST_NAME.toLowerCase.replaceAll("\\s", "").replace(",", "").replace(";", "")


  def getSongId(): String = this.SONG_ID

  def getTitle(): String = this.SONG_TITLE.toLowerCase.replaceAll("\\s","").replace(",","").replace(";","")


}

