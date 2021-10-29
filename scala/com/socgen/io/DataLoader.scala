package com.socgen.io

import com.socgen.format.Formatter
import com.socgen.format.formatters.Csv.CsvConf
import com.socgen.format.formatters.HiveTable.{HiveTableConf, HiveTableFormatter}
import com.socgen.format.formatters.Json.JsonConf
import com.socgen.format.formatters.Orc.OrcConf
import com.socgen.format.formatters.Parquet.ParquetConf
import com.socgen.sdt.logging.Logging
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Created by X178743 on 14/06/2019.
 * Copyright Société Générale.
 *
 * This Object is used to Read Different tables and File Format
 * It enables us to query a hive table with some specific conditions
 *
 */
object DataLoader extends Logging {

  /**
   * Takes a Input parms and Loads it into a DataFrame
   * according to input parameter.
   *
   * @param inputParams Object storing params values.
   * @return dataFrame DataFrame representing data from source file.
   */

  def load(inputParams: InputIOParams)(implicit spark: SparkSession): DataFrame = {
    info(s"Loading file configuration from ${inputParams.path} with format ${inputParams.format}")
    val dataFrame = getInputAsDataFrame(inputParams)
    dataFrame
  }


  private def getInputAsDataFrame(inputParams: InputIOParams)(implicit spark: SparkSession): DataFrame = {
    val format = inputParams.format
    val path = inputParams.path
    val delimiter = inputParams.delimiter
    val databaseTableName = inputParams.tableName

    format match {
      case InputType.CSV => loadCsvFile(path, delimiter)
      case InputType.JSON => Formatter.load(JsonConf(path))
      case InputType.ORC => Formatter.load(OrcConf(path))
      case InputType.PARQUET => Formatter.load(ParquetConf(path))
      case InputType.TABLE => val hiveConfiguration = HiveTableConf(
        databaseTableName,
        path,
        partitionedBy = inputParams.partitionColumn,
        partitions = inputParams.partitionValues)

        HiveTableFormatter.load(hiveConfiguration)
      case InputType.TEXT => loadTextFile(path, delimiter)
      case _ =>
        error(s"Input file type $format is not supported.")
        throw new Exception("Input file type is not supported.")
    }
  }

  /**
   * Takes a Input parms and conditions and Loads it into a DataFrame
   * according to input parameter and conditions.
   *
   * @param inputParams Object storing params values.
   * @param columns columns to load
   * @param conditionColumn column on which we apply the condition
   * @param minimalValue minmum value to put in condition
   * @return dataFrame DataFrame representing the result of the query.
   */

  def loadTableWithSpecificColumnAndCondition(inputParams: InputIOParams, columns : Seq[String], minimalValue : String, conditionColumn : String)(implicit spark: SparkSession): DataFrame = {
    info(s"Loading file configuration from ${inputParams.path} with format ${inputParams.format}")

    val path = inputParams.path
    val tableName = inputParams.tableName
    val hiveConfiguration = HiveTableConf(
      tableName,
      path,
      partitionedBy = inputParams.partitionColumn,
      partitions = inputParams.partitionValues)
    val dataFrame = HiveTableFormatter.loadSpecificColumnAndCondition(hiveConfiguration, columns, minimalValue, conditionColumn)
    dataFrame
  }

  /**
   * Takes a HDFS file directory with its delimiter and Loads a CSV File into a DataFrame
   * according to input parameter.
   *
   * @param filesDirectory CsvConf storing user's choices.
   * @param delimiter delimiter of the Csv File.
   * @return dataFrame DataFrame representing data from source file.
   */
  private def loadCsvFile(filesDirectory: String, delimiter: String)(implicit spark: SparkSession): DataFrame = {
    val parsedDelimiter = if (StringUtils.isEmpty(delimiter)) DataFrameIOOptions.CSV_DELIMITER else delimiter

    val csvConf = CsvConf(path = filesDirectory, delimiter = parsedDelimiter)
    Formatter.load(csvConf)
  }

  /**
   * Takes a Input HDFS file directory with its delimiter and Loads a text file
   * according to input parameter.
   *
   * @param filesDirectory CsvConf storing user's choices.
   * @param delimiter delimiter of the Csv File.
   * @return dataFrame DataFrame representing data from source file.
   */
  private def loadTextFile(filesDirectory: String, delimiter: String)(implicit spark: SparkSession): DataFrame = {
    val parsedDelimiter = if (StringUtils.isEmpty(delimiter)) DataFrameIOOptions.CSV_DELIMITER else delimiter

    val rdd = spark.sparkContext.textFile(filesDirectory)
    val parsedRdd = rdd.map(_.split(parsedDelimiter.charAt(0)).to[List]).map(x => Row(x: _*))

    val firstRow = rdd.take(1).flatMap(x => x.split(parsedDelimiter.charAt(0)))
    val structFields = firstRow.indices.foldLeft(List.empty[StructField])((structFields, indice) =>
      structFields :+ StructField(name = "col" + (indice + 1), dataType = StringType, nullable = true))
    val schema = StructType(structFields)

    spark.createDataFrame(parsedRdd, schema)
  }


}
