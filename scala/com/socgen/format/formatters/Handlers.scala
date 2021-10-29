package com.socgen.format.formatters

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Used to distinguish an InputHandler from an OutputHandler.
  */

object Handlers {

  trait InputHandler[T <: Configuration] {
    def load(conf: T)(implicit spark: SparkSession): DataFrame

    def split(df: DataFrame, repartition: Int): DataFrame = {
      if (repartition == -1) {
        df
      }
      else {
        df.repartition(repartition)
      }
    }
  }

  trait OutputHandler[T <: Configuration] {
    def save(conf: T, df: DataFrame)
  }

}
