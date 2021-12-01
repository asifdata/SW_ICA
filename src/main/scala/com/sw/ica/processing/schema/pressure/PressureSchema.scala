package com.sw.ica.processing.schema.pressure

/**
 * Schema case class for manual Pressure reading
 *
 *
 */
class PressureSchema {

    //Manual pressure reading data schema
    case class SchemaGeneral(
      year:             String,
      month:            String,
      day:              String,
      pressure_morning: String,
      pressure_noon:    String,
      pressure_evening: String)



  }