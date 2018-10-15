

//author: Jiangtao
class AverageSongInfo(ai: String, du: String, ld: String, sh: String, ah: String, tp: String, af: String) extends Serializable {
  var ARTIST_ID: String = ai
  var TEMPO: String = tp
  var SONG_HOTNESS: String = sh
  var ARTIST_HOT: String = ah
  var LOUDNESS: String = ld
  var DURATION: String = du
  var ARTIST_FAM: String = af


  def this(line: String) = {
    this("", "", "", "", "", "", "")
    def tokens = line.split(",")

    this.ARTIST_ID = tokens(0)
    this.TEMPO = tokens(1)
    this.SONG_HOTNESS = tokens(2)
    this.ARTIST_HOT = tokens(3)
    this.LOUDNESS = tokens(4)
    this.DURATION = tokens(5)
    this.ARTIST_FAM = tokens(6)
  }


  def getDuration(): String = this.DURATION

  def getLoudness(): String = this.LOUDNESS

  def getTempo(): String = this.TEMPO

  def getArtFam(): String = this.ARTIST_FAM

  def getArtHot(): String = this.ARTIST_HOT

  def getSongHot(): String = this.SONG_HOTNESS

  def getArtistId(): String = this.ARTIST_ID


}

