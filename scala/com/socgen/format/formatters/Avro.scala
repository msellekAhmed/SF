package com.socgen.format.formatters

import com.socgen.format.Formatter
import com.socgen.format.formatters.Handlers.{InputHandler, OutputHandler}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory


/**
  * Created by X178743 on 14/06/2019.
  * Copyright Société Générale.
  */
object Avro {

  /**
    * Variables (except path) have by default values
    * and can be set to custom values,
    *
    * @constructor : path: String used to load an input or save an output file,
    *              partitionBy: String representing column that should be used to partition output,
    *              repartition: Int, number of parts to split output DataFrame.
    */
  case class AvroConf(path: String, partitionBy: String = "", repartition: Int = -1) extends Configuration

  val logger = LoggerFactory.getLogger(classOf[Formatter])

  implicit object AvroFormatter extends InputHandler[AvroConf] with OutputHandler[AvroConf] {

    /**
      * Takes Avro source file pointed by class parameter conf
      * and converts it into DataFrame.
      *
      * @param conf ParquetConf storing user's choices.
      * @return DataFrame representing data from source file.
      */
    def load(conf: AvroConf)(implicit spark: SparkSession): DataFrame = {
      logger.info("Converts Avro file at " + conf.path + " into a DataFrame.")
      split(spark.read.format("com.databricks.spark.avro").load(conf.path), conf.repartition)
    }

    /**
      * Takes a DataFrame and saves it into an Avro file
      * creating partitions if necessary.
      * Attribute partitionBy of conf class parameter
      * indicates field used for partitioning.
      * This parameter also determines output path.
      *
      * @param conf ParquetConf storing user's choices.
      * @param df   DataFrame representing data from source file.
      */
    def save(conf: AvroConf, df: DataFrame) {
      logger.info("Saves DataFrame into an Avro file at " + conf.path + ".")
      val writer = df.write.mode(SaveMode.Append).format("com.databricks.spark.avro")
      if (!conf.partitionBy.equals("")) {
        logger.debug("Partitions Avro files by " + conf.partitionBy + ".")
        writer.partitionBy(conf.partitionBy)
      }
      writer.save(conf.path)
    }

  }

}