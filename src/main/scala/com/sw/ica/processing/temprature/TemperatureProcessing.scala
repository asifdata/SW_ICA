package com.sw.ica.processing.temprature

import com.sw.ica.utility.{Config, SparkController}
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit

import scala.util.Failure

/**
 * Spark program that transforms temperature data and stores it in hive table.
 *
 *
 */

class TemperatureProcessing extends SparkController {

  // logger
  val log = Logger.getLogger(getClass.getName)


  /*
   * Temperature Data Cleaning
   *
   * @param sparkSession
   */
  def processTemperature(sparkSession: SparkSession,config:Config): Unit = {

    var temperatureDataFrame = spark.read.format("csv").
      option("header", "false").
      option("delimiter", "\t").
      schema(TemperatureSchema).
      load(config.inputTemperatureSrc)

    try {
      // Reading input data

      // Add station column
      val manualStationDF = temperatureDataFrame.withColumn("station", lit("manual"))

      //creating dataframe and adding necessary columns

      val temperatureDataDF = temperatureDataFrame
        .withColumn("minimum", lit("NaN"))
        .withColumn("maximum", lit("NaN"))
        .withColumn("estimatedDiurnalMean", lit("NaN"))
        .withColumn("station", lit("NaN"))

      //----------------------------Save temperature data to hive table-----------------------------


      // Creating hive table
      sql("""CREATE TABLE TemperatureData(
        year String, 
        month String, 
        day String, 
        morning String, 
        noon String, 
        evening String, 
        minimum String,
        maximum String,
        estimated_diurnal_mean String, 
        station String)
      STORED AS PARQUET""")

      // Writing to hive table created above.
      temperatureDF.write.mode(SaveMode.Overwrite).saveAsTable("TemperatureData")


      //---------------------------------------Data Analysis---------------------------------



      log.info("Input data count is " + totalInputCount)
      log.info("Transformed input data count is " + temperatureDF.count())
      log.info("Hive data count " + sql("SELECT count(*) FROM TemperatureData").show(false))

    } catch {
      case fileNotFoundException: FileNotFoundException => {
        log.error("Input file not found")
        Failure(fileNotFoundException)
      }
      case exception: Throwable => {
        log.error("Exception found " + exception)
        Failure(exception)
      }
    }
  }
}