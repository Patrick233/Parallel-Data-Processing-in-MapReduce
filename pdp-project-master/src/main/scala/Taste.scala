import scala.util.Try
/*
 * @author Yang Xia, Jiangtao Xu, Yu Wen
 */
class Taste(jam : String, track : String, c : String) {
  var USER : String = jam
  var SONG : String = track
  var COUNT: String  = c

  def this(line : String) ={
    this("","","")
    def tokens = line.split("\t")
    this.USER = tokens(0)
    this.SONG = tokens(1)
    this.COUNT = tokens(2)
  }

  def getSong():String = this.SONG

  def getUser():String = this.USER

  def getCount(): Int = this.COUNT.toInt

  def isValidCount(): Boolean = Try(this.COUNT.toInt).isSuccess


}
