/*
 * @author Yang Xia, Jiangtao Xu, Yu Wen
 */
class Jam(jam : String, track : String) {
  var JAM_ID : String = jam
  var TRACK_ID : String = track

  def this(line : String) ={
    this("","")
    def tokens = line.split("\t")
    this.JAM_ID = tokens(0)
    this.TRACK_ID = tokens(1)
  }

  def getJam():String = this.JAM_ID

  def getTrack():String = this.TRACK_ID


}
