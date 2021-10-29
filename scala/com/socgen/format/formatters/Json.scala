package com.socgen.format.formatters

import com.socgen.format.Formatter
import com.socgen.format.formatters.Handlers.{InputHandler, OutputHandler}
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
  * Created by X178743 on 14/06/2019.
  * Copyright Société Générale.
  */
/**
  * Allows to create a JsonConf
  * and use a JsonFormatter to load and save JSON files.
  */
object Json {

  private val OPTION_TIMESTAMP_FORMAT = "timestampFormat"
  private val TIMESTAMP_FORMAT = "yyyy/MM/dd HH:mm:ss ZZ"
  private val NO_PARTITION = ""
  private val JSON_FORMAT = "json"
  private val logger = LoggerFactory.getLogger(classOf[Formatter])

  /**
    * Variables (except path) have by default values
    * and can be set to custom values.
    *
    * @constructor : path: String used to load an input or save an output file,
    *              schema: StructType, by default schema is inferred but this parameter allows to use custom schema,
    *              repartition: Int, number of parts to split output DataFrame.
    */
  case class JsonConf(path: String, schema: StructType = null, partitionBy: String = NO_PARTITION, repartition: Int = -1) extends Configuration



  implicit object JsonFormatter extends InputHandler[JsonConf] with OutputHandler[JsonConf] {
    /**
      * Takes source file pointed by class parameter conf
      * and converts it into a DataFrame.
      *
      * @param conf JsonConf storing user's choices.
      * @return DataFrame representing data from source file.
      */
    def load(conf: JsonConf)(implicit spark: SparkSession): DataFrame = {
      logger.info("Converts " + JSON_FORMAT + " file at " + conf.path + " into a DataFrame.")
      split(spark.read.option(OPTION_TIMESTAMP_FORMAT, TIMESTAMP_FORMAT).json(conf.path), conf.repartition)
    }

    /**
      * This method must be implemented by all class extending InputHandler.
      * Saves data from a DataFrame into a JSON file.
      *
      * @param conf JsonConf storing user's choices.
      * @param df   DataFrame representing data from source file.
      */
    def save(conf: JsonConf, df: DataFrame): Unit = {
      val outputPath = conf.path
      logger.info("DataFrame saved into a JSON file at " + outputPath + ".")
      val jsonWriter = df.write.mode(Append).format(JSON_FORMAT)
      val partitionColumn = conf.partitionBy
      if (!StringUtils.isEmpty(partitionColumn)) {
        jsonWriter.partitionBy(partitionColumn)
    }
      jsonWriter.save(outputPath)
    }
  }

}
