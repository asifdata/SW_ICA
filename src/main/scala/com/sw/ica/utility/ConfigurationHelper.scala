package com.sw.ica.utility
import org.json4s.jackson.JsonMethods.parse
import org.json4s.DefaultFormats
import scala.io.Source

case class Config(inputPressureSrc:String,inputTemperatureSrc:String,pressureColumnMapping:List[String],temperatureColumnMapping:List[String])

/**
 * ConfigurationHelper act as a configuration handler
 */
object ConfigurationHelper {

  implicit val formats = DefaultFormats
  def getConfigFileContent(filename:String):String={
    val fileContents = Source.fromFile(filename).getLines.mkString
    return  fileContents
  }
  //Converting String content as json format
  def getConfigAsJson(fileContents:String):Config={
    val configJson = parse(fileContents).extract[Config]
    configJson
  }

}
