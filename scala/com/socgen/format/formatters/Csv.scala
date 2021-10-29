package com.socgen.format.formatters

import com.socgen.format.Formatter
import Handlers.{InputHandler, OutputHandler}
import com.socgen.format.options.{DefaultValue, SparkOption}
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
  * Allows to create a CsvConf
  * and use a CsvFormatter to load and save CSV files.
  */
object Csv {

  private val FORMAT_CSV = "com.databricks.spark.csv"

  /**
    * Variables (except path) have by default values
    * and can be set to custom values.
    *
    * @constructor : path: String used to load an input or save an output file,
    *              useHeader: Boolean, when is true first line of file is used to name columns and is not part of data,
    *              delimiter: String used to delimit columns,
    *              schema: StructType, by default schema is inferred but this parameter allows to use custom schema,
    *              partitionBy: String representing column that should be used to partition output
    *              into several files according to values of this column,
    *              repartition: Int, number of parts to split output DataFrame.
    */
  case class CsvConf(path: String,
                     useHeader: Boolean = DefaultValue.USE_HEADER,
                     delimiter: String = DefaultValue.DELIMITER,
                     schema: StructType = null,
                     partitionBy: String = DefaultValue.PARTITION_BY,
                     repartition: Int = DefaultValue.REPARTITION) extends Configuration

  val logger = LoggerFactory.getLogger(classOf[Formatter])

  implicit object CsvFormatter extends InputHandler[CsvConf] with OutputHandler[CsvConf] {

    /**
      * Takes CSV source file pointed by class parameter conf
      * and converts it into DataFrame.
      * During conversion method applies many treatments
      * according to conf (delimiter, header, schema).
      *
      * @param configuration CsvConf storing user's choices.
      * @return DataFrame representing data from source file.
      */
    def load(configuration: CsvConf)(implicit spark: SparkSession): DataFrame = {
      logger.info("Converts CSV file at " + configuration.path + " into a DataFrame.")
      logger.debug("Delimiter is " + configuration.delimiter + ".")
      logger.debug("Uses a " + SparkOption.HEADER + " = " + configuration.useHeader.toString + ".")
      val reader = spark.read
        .format(FORMAT_CSV)
        .option(SparkOption.DELIMITER, configuration.delimiter)
        .option(SparkOption.HEADER, configuration.useHeader.toString)
      if (configuration.schema != null) {
        logger.debug("Uses a custom schema.")
        reader.schema(configuration.schema)
      }
      else {
        logger.debug("Infers a schema.")
        reader.option("inferSchema", "true")
      }

      split(reader.load(configuration.path), configuration.repartition)
    }

    /**
      * Takes a DataFrame and saves it into a CSV file
      * according to path from conf parameter.
      *
      * @param configuration CsvConf storing user's choices.
      * @param dataFrame     DataFrame representing data from source file.
      */
    def save(configuration: CsvConf, dataFrame: DataFrame): Unit = {
      val writer = dataFrame.write
        .mode(SaveMode.Append)
        .format(FORMAT_CSV)
        .option(SparkOption.DELIMITER, configuration.delimiter)
        .option(SparkOption.HEADER, "false")
      if (StringUtils.isNotEmpty(configuration.partitionBy)) {
        writer.partitionBy(configuration.partitionBy)
        logger.debug("Partitions Parquet files by " + configuration.partitionBy + ".")
      }

      writer.save(configuration.path)
      logger.info("DataFrame saved into a CSV file at " + configuration.path + ".")
    }

  }

}
