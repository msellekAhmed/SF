package com.socgen.utils

import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import java.util.Calendar

import com.socgen.io._
import com.socgen.poc.Anonymise
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}

import scala.collection.Seq
import scala.util.{Failure, Success}



/**
 * Created by X178743 on 14/06/2019.
 * Copyright Société Générale.
 *
 * This Object id to implement Helpers computation used for desensitization
 *
 */
object DesensitizationUtilities {

  private lazy val logger = LogManager.getLogger(this.getClass.getName)
  implicit val spark: SparkSession = Anonymise.spark
  private val DELIMITER_DOT = "."
  private val DESENSITIZED = "_desensitized/"
  private val BACK_SLASH = "/"
  private val UNDERSCORE = "_"

  /**
   * This UDF is Used to return the Year and the Month of the trigger column acording to its format.
   * Used especially when trigger event is other than execution date
   *
   * @param valueFormat Value Format of the Date.
   * @param processColumnValue   Value of the column used for calculation.
   *
   */
  private def reduceTriggerColumnValue(valueFormat : String, processColumnValue : String) : String = {
    valueFormat match {
      case "yyyyMMdd" => processColumnValue.substring(0, 6)
      case "yyyy-MM-dd" => processColumnValue.substring(0, 8)
      case _ => ""
    }
  }
  def reduceProcessColumnValueUDF = functions.udf(reduceTriggerColumnValue _)


  /**
   * Used to Save the desensitized Data.
   *
   *
   * @param databaseTableName Data Base And Table Name of the Desensitized Data.
   * @param firstConfigDFRow  First Row of the configuration DF.
   * @param partitionsToDesensitize Desensitized Data Partitions.
   * @param desensitizedData   Desensitized Data.
   */
  def saveDesensitizedData(databaseTableName: String, firstConfigDFRow: Row, partitionsToDesensitize: Seq[String], desensitizedData: DataFrame) = {
    var desensitizedTableName : String = ""
    if (databaseTableName.length > 1) desensitizedTableName = databaseTableName.split("\\" + DELIMITER_DOT)(1).concat(DESENSITIZED)
    val outputPath = firstConfigDFRow.getAs(TriggerTableColumn.outputPath).toString
    val outputPartitionColumn = firstConfigDFRow.getAs(TriggerTableColumn.outputPartitionColumn).toString
    val outputFormat = firstConfigDFRow.getAs(TriggerTableColumn.outputFormat).toString
    val desensitizedTableParams = OutputIOParams(outputFormat, desensitizedTableName, outputPath, outputPartitionColumn, DataFrameIOOptions.DEFAULT_DELIMITER, partitionsToDesensitize)
    DataWriter.save(desensitizedData, desensitizedTableParams)
  }


  /**
   * get Global Trigger Table Partitions from which we calculate partitions to desensitize.
   * Used when trigger event is other than execution date
   *
   * @param triggerTableDF Trigger Table DataFrame.
   * @return triggerPartition set of trigger partitions
   */
  def getTriggerPartitions(triggerTableDF: DataFrame) = {
    val truncatedTriggerTableDF = triggerTableDF.withColumn(TriggerTableColumn.inputTriggerColumnProcess, DesensitizationUtilities.reduceProcessColumnValueUDF(triggerTableDF(TriggerTableColumn.inputTriggerColumnProcessValueFormat), triggerTableDF(TriggerTableColumn.inputTriggerColumnProcess)))
    val TriggerPartitions = truncatedTriggerTableDF.select(TriggerTableColumn.inputTriggerColumnProcess).distinct().collect().map(_.getAs(TriggerTableColumn.inputTriggerColumnProcess).toString().trim).toSeq
    TriggerPartitions
  }

  /**
   * get Partitions To Desensitize From Trigger Table from trigger Partitions
   * According to the given conservation duration
   * Used when trigger event is other than execution date
   *
   * @param triggerPartition partition of the Trigger Table.
   * @param triggerPartitionValueFormat Value Format of the Trigger Table Partitions.
   * @param actualDate Actual Date.
   * @param conservationDuration Conservation Duration.
   * @param outPutDateValueFormat Output Date Value Format.
   * @return a list of the partition to desensitize for the given configurations
   *
   */
  def getPartitionsToDesensitize(triggerPartition : String, triggerPartitionValueFormat : String, actualDate : String, conservationDuration : String, outPutDateValueFormat : String) : Seq[String] = {

    val dateFormatter = new SimpleDateFormat(outPutDateValueFormat)
    val dateCalendar = Calendar.getInstance()
    val actualDateMonth = actualDate.substring(4)
    val actualDateYear = actualDate.substring(0,4)
    val j = LocalDate.of(actualDateYear.toInt, actualDateMonth.toInt, 1)
    val triggerPartitionDateFormatter = new SimpleDateFormat(triggerPartitionValueFormat)
    val dateMonth = triggerPartition.substring(4)
    val dateYear = triggerPartition.substring(0,4)
    val triggerPartitionCompleteDate = triggerPartitionValueFormat match {
      case "yyyyMMdd" => triggerPartition.concat("00")
      case "yyyy-MM-dd" => dateYear.concat("-").concat(dateMonth).concat("-").concat("00")
    }
    val triggerPartitionDate = triggerPartitionDateFormatter.parse(triggerPartitionCompleteDate)

    val exec = LocalDate.of(dateYear.toInt, dateMonth.toInt, 1)
    dateCalendar.setTime(triggerPartitionDate)

    val period = Period.between(exec, j)
    val monthBetween = period.getMonths
    val yearBetween = period.getYears
    val totalMonthBetween = monthBetween + 12 * yearBetween

    println(totalMonthBetween)

    if( conservationDuration.toInt > totalMonthBetween) {
      println(false)
      Seq.empty[String]
    }
    else {
      val partitionsToDesensitize = totalMonthBetween - conservationDuration.toInt
        println(true)
      for (month <- 1 to partitionsToDesensitize) yield {
        dateCalendar.add(Calendar.MONTH, 1)
        dateFormatter.format(dateCalendar.getTime)
      }
    }
  }


  /**
   * get Ids of clients and Partitions of Data To Desensitize
   * according to the params
   * Used when trigger event is other than execution date
   *
   * @param tableToDesensitizeCompleteName Data Base And Table Name of the Table to Desensitize.
   * @param tablesToDesensitizeJoinColumn Table to Desensitize Join Column Name used for trigger table join.
   * @param triggerTableDF DataFrame of the Trigger Table.
   * @param processColumn Trigger Table Process Column Name .
   * @param joinColumn Trigger Table Join Column Name.
   * @param partitionsToDesensitize Global Partitions to Desensitize.
   * @return (ids, partitions) ids of the persons and partition to desensitize
   *
   */
  def getIdsAndPartitionsToDesensitize(tableToDesensitizeCompleteName : String, tablesToDesensitizeJoinColumn : String, triggerTableDF : DataFrame, processColumn : String, joinColumn : String, partitionsToDesensitize : Seq[String])(implicit spark: SparkSession) = {

    val maybeQuery = partitionsToDesensitize match {
      case firstPartition +: nextPartitions =>
        val triggerTableViewName = "triggerTable"
        triggerTableDF.createOrReplaceTempView(triggerTableViewName)
        val select = s"""SELECT $tableToDesensitizeCompleteName.$tablesToDesensitizeJoinColumn, $triggerTableViewName.$processColumn, $tableToDesensitizeCompleteName.dt FROM $tableToDesensitizeCompleteName LEFT JOIN $triggerTableViewName ON $tableToDesensitizeCompleteName.$tablesToDesensitizeJoinColumn = $triggerTableViewName.$joinColumn """
        val where = nextPartitions.foldLeft(s"WHERE $processColumn='$firstPartition'") {
          (query, partition) => query + s" OR $processColumn='$partition'"
        }
        Success(s"$select $where")
      case Nil =>
        Failure(new IllegalArgumentException("You must define partitions argument : " +
          "partitionedBy argument is defined but partitions argument is empty."))
    }
    val idsAndPartitionsDF = maybeQuery match {
      case Success(query) =>
        println(query)
        spark.sql(query)
      case Failure(error) => throw error
    }
    val ids  = idsAndPartitionsDF.select(idsAndPartitionsDF(tablesToDesensitizeJoinColumn)).distinct().collect().map( row => row.getAs(tablesToDesensitizeJoinColumn).toString.trim)
    val partitions = idsAndPartitionsDF.select(idsAndPartitionsDF(DataFrameIOOptions.DEFAULT_PARTITION_COLUMN)).distinct().collect().map(row => row.getAs(DataFrameIOOptions.DEFAULT_PARTITION_COLUMN).toString.trim).toSeq
    (ids, partitions)
  }

  /**
   * get Data And data to Desensitize given Row Ids and Paritions.
   * Used when trigger event is other than execution date
   *
   * @param idsAndPartitions Ids and Paritions of the Rows to Desensitize .
   * @param databaseTableName Data Base And Table Name of the Table to Desensitize.
   * @param firstConfigDFRow Table to Desensitized Row of the configuration DF.
   * @return (filteredTableToDesensitize, filteredTable) the first Df represents the data to desensitize and te rest is
   *         the Data to keep without desensitization
   */
  def getDataToDesensitize(idsAndPartitions : (Seq[String], Seq[String]), databaseTableName: String, firstConfigDFRow: Row) : (DataFrame,DataFrame) = {
    val partitions = idsAndPartitions._2
    val ids = idsAndPartitions._1
    val inputPath = firstConfigDFRow.getAs(TriggerTableColumn.inputPathParam).toString() trim
    val inputFormat = firstConfigDFRow.getAs(TriggerTableColumn.inputFormat) toString() trim
    val partitionColumn = firstConfigDFRow.getAs(TriggerTableColumn.inputPartitionColumn) toString() trim
    val partitionValueFormat = firstConfigDFRow.getAs(TriggerTableColumn.inputPartitionValue) toString() trim
    val partitionsToDesensitize = for ( part <- partitions) yield {
      DateUtilities.getPartitionsToDesensitize(partitionValueFormat, part)
    }
    val flattenedPartitionsToDesensitize = partitionsToDesensitize.flatten
    logger.info(s"Calculated Partitions to Anonymize  are : ${flattenedPartitionsToDesensitize}")
    val tableToDesensitizeParams = InputIOParams(inputFormat, databaseTableName, inputPath, partitionColumn, DataFrameIOOptions.DEFAULT_DELIMITER, flattenedPartitionsToDesensitize)
    val tableToDesensitize = DataLoader.load(tableToDesensitizeParams).persist()
    val filteredTableToDesensitize = tableToDesensitize.filter( tableToDesensitize("") isin ids)
    val filteredTable = tableToDesensitize.except(filteredTableToDesensitize)
    (filteredTableToDesensitize, filteredTable)
  }


  /**
   * Apply Desensitization method to the table's Column.
   *
   *
   * @param configDF Table to Desensitized Row of the configuration DF.
   * @param tableToDesensitize Database and Table name of the data to Desensitize.
   * @return desensitizedData DataFrame that represents the desensitized data
   */
  def desensitizeTableColumns(configDF : DataFrame, tableToDesensitize : DataFrame) : DataFrame = {
    var desensitizedData = tableToDesensitize
    configDF collect() foreach(
      row => {
        val desensitizationMethod = row.getAs(TriggerTableColumn.inputDesensitisationMethod).toString
        val desensitizationMethodsOptions: List[String] = row.getAs(TriggerTableColumn.inputDesensitisationMethodOptions).toString.trim.split(DELIMITER_DOT).toList
        val desensitisationMethodsOptionsValues: List[String] = row.getAs(TriggerTableColumn.inputDesensitisationMethodOptionsValues).toString.trim.split(DELIMITER_DOT).toList
        val columnName = row.getAs(TriggerTableColumn.inputColumnName).toString
        logger.info("Showing the Data Frame of the Partitions to anonymise :")
        desensitizedData = DataFrameUtilities.handleDFNullValues(desensitizedData, columnName)
        desensitizedData.show()
        val crossOptions = desensitizationMethodsOptions zip desensitisationMethodsOptionsValues
        logger.info(s"Starting Desensitization of ${columnName}")
        val desensitizationFunction: (DataFrame, String) => DataFrame = {
          Desensitizer.desensitizeDataFrame(desensitizationMethod, Option(crossOptions))
        }
        val desensitizedTable = desensitizationFunction(desensitizedData, columnName).persist()
        desensitizedData = desensitizedTable
      })
    desensitizedData
  }


}
