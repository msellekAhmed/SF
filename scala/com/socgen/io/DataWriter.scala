package com.socgen.io

import com.socgen.format.Formatter
import com.socgen.format.formatters.Csv.CsvConf
import com.socgen.format.formatters.Json.JsonConf
import com.socgen.format.formatters.Orc.OrcConf
import com.socgen.format.formatters.Parquet.ParquetConf
import com.socgen.sdt.logging.Logging
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created by X178743 on 14/06/2019.
 * Copyright Société Générale.
 *
 * This Object is used to Read Different tables and File Format
 * It enables us to query a hive table with some specific conditions
 *
 */

object DataWriter extends Logging {

  private val WITH_HEADER = true
  private val WITHOUT_HEADER = false

  /**
   * Takes as Input a dataframe and HDFS output params and Save it into specific file format
   * according to input parameter.
   *
   * @param outputParam CsvConf storing user's choices.
   * @param dataFrame dataframe that represents Data to store
   * @return dataFrame DataFrame representing data from source file.
   */
  def save(dataFrame: DataFrame, outputParam : OutputIOParams)(implicit spark: SparkSession): Unit = {
    val path = outputParam.path
    info(s"Saving data set to file $path with format ${outputParam.format} partitioned by ${outputParam.partitionColumn}")
    val parsedDelimiter = {
      if (StringUtils.isEmpty(outputParam.delimiter)) {
        DataFrameIOOptions.CSV_DELIMITER
      }
      else {
        outputParam.delimiter
      }
    }
    val partitionBy = outputParam.partitionColumn

    outputParam.format match {
      case OutputType.CSV =>
        val csvConf = CsvConf(path = path, delimiter = parsedDelimiter, useHeader = WITH_HEADER, partitionBy = partitionBy)
        Formatter.save(csvConf, dataFrame)
      case OutputType.CSV_WITHOUT_HEADER =>
        val csvConf = CsvConf(path = path, delimiter = parsedDelimiter, useHeader = WITHOUT_HEADER, partitionBy = partitionBy)
        Formatter.save(csvConf, dataFrame)
      case OutputType.JSON =>
        val jsonConf = JsonConf(path = path, partitionBy = partitionBy)
        Formatter.save(jsonConf, dataFrame)
      case OutputType.ORC =>
        val orcConf = OrcConf(path = path, partitionBy = partitionBy)
        Formatter.save(orcConf, dataFrame)
      case OutputType.PARQUET =>
        val parquetConf = ParquetConf(path = path, partitionBy = partitionBy)
        Formatter.save(parquetConf, dataFrame)
      case _ =>
        error(s"Output file type ${outputParam.format} is not supported.")
        throw new Exception("Output file type is not supported.")
    }
  }

}
