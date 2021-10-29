package com.socgen.format.formatters

import com.socgen.format.Formatter
import com.socgen.format.formatters.Handlers.{InputHandler, OutputHandler}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
  * Created by X178743 on 14/06/2019.
  * Copyright Société Générale.
  */
/**
  * Allows to create a ParquetConf
  * and use a ParquetFormatter to load and save Parquet files.
  */
object Parquet {

  /**
    * Variables (except path) have by default values
    * and can be set to custom values.
    *
    * @constructor : path: String used to load an input or save an output file,
    *              schema: StructType, by default schema is inferred but this parameter allows to use custom schema,
    *              partitionBy: String representing column that should be used to partition output
    *              into several files according to values of this column,
    *              repartition: Int, number of parts to split output DataFrame.
    */
  case class ParquetConf(path: String, schema: StructType = null, partitionBy: String = "", repartition: Int = -1) extends Configuration

  val logger = LoggerFactory.getLogger(classOf[Formatter])

  implicit object ParquetFormatter extends InputHandler[ParquetConf] with OutputHandler[ParquetConf] {

    /**
      * Takes Parquet source file pointed by class parameter conf
      * and converts it into DataFrame.
      *
      * @param conf  ParquetConf storing user's choices.
      * @param spark SparkSession as implicit parameter.
      * @return DataFrame representing data from source file.
      */
    def load(conf: ParquetConf)(implicit spark: SparkSession): DataFrame = {
      logger.info("Converts Parquet file at " + conf.path + " into a DataFrame.")
      split(spark.read.parquet(conf.path), conf.repartition)
    }

    /**
      * Takes a DataFrame and saves it into a Parquet file
      * creating partitions if necessary.
      * Attribute partitionBy of conf class parameter
      * indicates field used for partitioning.
      * This parameter also determines output path.
      *
      * @param conf ParquetConf storing user's choices.
      * @param df   DataFrame representing data from source file.
      */
    def save(conf: ParquetConf, df: DataFrame) {
      logger.info("Saves DataFrame into a Parquet file at " + conf.path + ".")
      val writer = df.write.mode(SaveMode.Append).format("Parquet")
      if (!conf.partitionBy.equals("")) {
        logger.debug("Partitions Parquet files by " + conf.partitionBy + ".")
        writer.partitionBy(conf.partitionBy)
      }
      writer.save(conf.path)
    }

  }

}