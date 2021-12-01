package com.sw.ica.utility

import scala.util.Failure
import org.apache.log4j.Logger


object EntryPointInitializer extends SparkController {
  val log = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    val filePath = args(0)

    log.info("Initializing Processing framework ")
    try{
      val  configContent =  ConfigurationHelper.getConfigFileContent(filePath)
      val configJson = ConfigurationHelper.getConfigAsJson(configContent)

      //Processing Pressure data
      PressureProcessing.processPressureData(spark,configJson)
      //Processing Temperature data
      TemperatureProcessing.processPressureData(spark,configJson)
      log.info("Stopping Spark Processing framework ")
      spark.stop()
    }catch {
      case fileNotFoundException: FileNotFoundException => {
        log.error("Input file not found")
        Failure(fileNotFoundException)
      }
      case exception: Throwable => {
        log.error("Exception found " + exception)
        Failure(exception)
      }
    }finally{
      spark.stop()
    }

  }
}
