package com.sw.ica.utility
import org.apache.spark.sql.SparkSession
/*
 * Spark trait controller which help to control spark session
 * helper method which help to stop session when ever session end
 */

trait SparkController {

  //Spark session which lazy session
  lazy val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

  //Method to stop the spark session
  def stopSpark(): Unit = spark.stop()
}

