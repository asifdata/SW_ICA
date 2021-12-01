package com.sw.ica.processing.pressure

import com.sw.ica.utility.{Config, ConfigurationHelper, SparkController}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Failure


/**
 * Spark program that transforms pressure data and stores it in hive table.
 *
 *
 */

  class PressureProcessing extends SparkController {

  // logger
  val log = Logger.getLogger(getClass.getName)

  /*
   * Pressure weather data transformation and analysis
   *
   * @param sparkSession
   */
  def processPressureData(sparkSession: SparkSession,config:Config): Unit = {

    try {

      //Reading input data


      var pressureDataFrame = spark.read.format("csv").
        option("header", "false").
        option("delimiter", "\t").
        schema(PressureSchema.SchemaGeneral).
        load(config.inputPressureSrc)

      //creating dataframe and adding necessary columns
      val manualPressureSchemaDF = pressureDataFrame
        .withColumn("station", lit("manual"))
        .withColumn("pressure_unit", lit("hpa"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))

      //selecting the required columns in dataframe
      val PressureDataDF = manualPressureSchemaDF.select(config.pressureColumnMapping)


     //----------------------------Save pressure data to hive table---------------------------------

      //creating hive table
      sql("""CREATE TABLE PressureData(
            year String, 
            month String, 
            day String, 
            pressure_morning String, 
            pressure_noon String, 
            pressure_evening String, 
            station String, 
            pressure_unit String, 
            barometer_temperature_observations_1 String, 
            barometer_temperature_observations_2 String, 
            barometer_temperature_observations_3 String, 
            thermometer_observations_1 String, 
            thermometer_observations_2 String, 
            thermometer_observations_3 String, 
            air_pressure_reduced_to_0_degC_1 String, 
            air_pressure_reduced_to_0_degC_2 String, 
            air_pressure_reduced_to_0_degC_3 String) 
          STORED AS PARQUET""")

      //writing to hive table created above
      PressureDataDF.write.mode(SaveMode.Overwrite).saveAsTable("PressureData")

     log.info("Hive data count " + sql("SELECT count(*) as count FROM PressureData").show(false))

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