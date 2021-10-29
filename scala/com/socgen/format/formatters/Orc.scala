package com.socgen.format.formatters

import com.socgen.format.Formatter
import com.socgen.format.formatters.Handlers.{InputHandler, OutputHandler}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
  * Created by X178743 on 14/06/2019..
  * Copyright Société Générale.
  */
object Orc {

  case class OrcConf(path: String, schema: StructType = null, partitionBy: String = "", repartition: Int = -1) extends Configuration

  val logger = LoggerFactory.getLogger(classOf[Formatter])

  implicit object OrcFormatter extends InputHandler[OrcConf] with OutputHandler[OrcConf] {

    /**
      * Takes Parquet source file pointed by class parameter conf
      * and converts it into a DataFrame.
      *
      * @return DataFrame representing data from source file.
      */
    def load(conf: OrcConf)(implicit spark: SparkSession): DataFrame = {
      logger.info("Converts ORC file at " + conf.path + " into a DataFrame.")
      split(spark.read.format("orc").load(conf.path), conf.repartition)
    }

    /**
      * Takes a DataFrame and saves it into an ORC file
      * creating partitions if necessary.
      * Attribute partitionBy of conf class parameter
      * indicates field used for partitioning.
      * This parameter also determines output path.
      *
      * @param df DataFrame representing data from source file.
      */
    def save(conf: OrcConf, df: DataFrame) {
      logger.info("Saves DataFrame into an ORC file at " + conf.path + ".")
      val writer = df.write.mode(SaveMode.Append).format("orc")
      if (!conf.partitionBy.equals("")) {
        logger.debug("Partitions ORC files by " + conf.partitionBy + ".")
        writer.partitionBy(conf.partitionBy)
      }
      writer.save(conf.path)
    }
  }

}