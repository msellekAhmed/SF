package com.socgen.format

import com.socgen.format.formatters.Handlers.{InputHandler, OutputHandler}
import com.socgen.format.formatters.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory


/**
  * Created by Franck Cussac on 15/05/2017.
  */
/**
  * Signature only used for logger factory.
  */
private class Formatter

/** Converts a file into an other file with same data
  * but a different format.
  * It can also be used to create a DataFrame with same data
  * as the input file.
  * Formatter is launched with convert() method.
  *
  */
object Formatter {

  val logger = LoggerFactory.getLogger(classOf[Formatter])

  /**
    * Loads a file according to inputConf
    * and saves it according to outputConf.
    *
    * @param inputConf  Configuration used by load function.
    * @param outputConf Configuration used by save fucntion.
    * @param input      Implicit parameter used to load file.
    * @param output     Implicit parameter used to save file.
    * @param spark      Implicit Spark Session.
    */
  def convert[U <: Configuration, V <: Configuration](inputConf: U, outputConf: V)(implicit input: InputHandler[U], output: OutputHandler[V], spark: SparkSession): Unit = {
    save(outputConf, load(inputConf))
  }

  /**
    * Converts file into a DataFrame.
    *
    * @param conf  Configuration storing user's choices for loading.
    * @param input Implicit InputHandler implementing load function.
    * @return DataFrame storing data from input.
    */
  def load[T <: Configuration](conf: T)(implicit input: InputHandler[T], spark: SparkSession): DataFrame = {
    input.load(conf)
  }

  /**
    * Saves this DataFrame into an output file.
    *
    * @param conf   Configuration storing user's choices for saving.
    * @param output Implicit OutputHandler implementing save function.
    */
  def save[T <: Configuration](conf: T, df: DataFrame)(implicit output: OutputHandler[T]): Unit = {
    output.save(conf, df)
  }

}
