/**
  * Created by Patrizio on 2017/11/27.
  */
class DownloadInfo(row:String) extends java.io.Serializable {

  val line: Array[String] = row.split(";")
  var artist = ""
  var title = ""
  var meanPrice = 0.0
  var download = 0
  var confidence = ""
  var combinedKey = ""
  var isValid = true

  try {
    artist = line(0)
    title = line(1)
    meanPrice = line(2).toDouble
    download = line(3).toInt
    confidence = line(4)
  } catch {
    case e: Exception => isValid = false
  }

  def getArtist(): String = this.artist.toLowerCase.replaceAll("\\s","").replace(",","").replace(";","")

  def getTitle(): String = this.title.toLowerCase.replaceAll("\\s","").replace(",","").replace(";","")

  def getMeanPrice(): Double = this.meanPrice

  def getDownload(): Int = this.download

  def getConfidence(): String = this.confidence


  def checkValidity():Boolean ={

    var result = false
    if (isValid && getMeanPrice() > 0
      && getDownload()>0
      && !getTitle().equals("") && !getArtist().equals("") && !getConfidence().equals("")) {
      result = true
    }
    result

  }


}

