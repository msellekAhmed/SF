package com.socgen.utils

import com.socgen.io.{DataFrameIOOptions, DataLoader, InputIOParams}
import com.socgen.poc.Anonymise
import org.apache.log4j.LogManager
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}

import scala.collection.Seq


/**
 * Created by X178743 on 14/06/2019.
 * Copyright Société Générale.
 *
 * This Object is used to implement different dataFrame utilities
 * It enables us to construct monitoring and Trigger Tables DataFrames
 *
 */
object DataFrameUtilities {


  private val DELIMITER_DOT = "."
  private val MONITORING_DATE_FORMAT = "dd/MM/yyyy hh:mm:ss" + DELIMITER_DOT + "SSS"
  private val MONITORING_DATE_PARTITION_FORMAT = "yyyy-MM-dd"
  implicit val spark: SparkSession = Anonymise.spark

  private lazy val logger = LogManager.getLogger(this.getClass.getCanonicalName)


  /**
   * Returns A General Single Row that contains information regarding desensitization of a Table
   * according to desensitized data and informations
   *
   * @param principal The user principal who runs the application .
   * @param desensitizedPartitions List of the desensitized partitions.
   * @param inputRowCount Row count before desensitisation.
   * @param desensitisationStartDate Desensitization start Date.
   * @param configurationDF Configuration DF of the Desensitized Table.
   * @param databaseTableName Tale and Database name of the Desensitized Data.
   * @param desensitisationStartMillisecond Desensitization end Date.
   * @param desensitizedData Desensitized Data.
   * @return monitoringData
   */
  def getMonitoringDataRowDF(principal: String, configurationDF: DataFrame, databaseTableName: String, desensitizedPartitions: Seq[String], inputRowCount: String, desensitisationStartMillisecond: Long, desensitisationStartDate: String, desensitizedData: DataFrame) = {
    val outputRowCount = desensitizedData.count().toString
    val desensitizationEndMillisecond = System.currentTimeMillis()
    val desensitizationEndDate = DateUtilities.formatDateTime(MONITORING_DATE_FORMAT, desensitizationEndMillisecond)
    val monitoringDatePartition = DateUtilities.formatDateTime(MONITORING_DATE_PARTITION_FORMAT, desensitizationEndMillisecond)
    val processingTime = ((desensitizationEndMillisecond - desensitisationStartMillisecond) / 1000).toString

    logger.info("****************************************************************************** Getting Monitoring Info *****************************************************************************************************")
    val tableName = databaseTableName.split("\\" + DELIMITER_DOT)(1)
    val dataBaseName =  databaseTableName.split("\\" + DELIMITER_DOT)(0)
    val aggregatedMonitoringRow = DataFrameUtilities.getAggregatedMonitoringData(configurationDF, tableName, dataBaseName)
    val monitoringData: DataFrame = DataFrameUtilities.buildMonirtoringRow(principal, desensitizedPartitions.toArray, inputRowCount, desensitisationStartDate, outputRowCount, desensitizationEndDate, monitoringDatePartition, processingTime, aggregatedMonitoringRow)
    monitoringData
  }

  /**
   * Returns A General Single Row that contains information regarding desensitization of a Table
   * according to desensitized data and informations
   *
   * @param principal The user principal who runs the application .
   * @param PurgedPartitions List of the desensitized partitions.
   * @param inputRowCount Row count before desensitisation.
   * @param purgeStartDate Desensitization start Date.
   * @param configurationDF Configuration DF of the Desensitized Table.
   * @param databaseTableName Tale and Database name of the Desensitized Data.
   * @param purgeStartMillisecond Desensitization end Date.
   * @param outputRowCount line count.
   * @return monitoringData
   */
  def getPurgeMonitoringDataRowDF(principal: String, configurationDF: DataFrame, databaseTableName: String, PurgedPartitions: Seq[String], inputRowCount: String, purgeStartMillisecond: Long, purgeStartDate: String, purgeEndMillisecond: Long, purgeEndDate: String, outputRowCount: String) = {

    val monitoringDatePartition = DateUtilities.formatDateTime(MONITORING_DATE_PARTITION_FORMAT, purgeEndMillisecond)
    val processingTime = ((purgeEndMillisecond - purgeStartMillisecond) / 1000).toString

    logger.info("****************************************************************************** Getting Purge Monitoring Info *****************************************************************************************************")
    val tableName = databaseTableName.split("\\" + DELIMITER_DOT)(1)
    val dataBaseName =  databaseTableName.split("\\" + DELIMITER_DOT)(0)
    val aggregatedMonitoringRow = DataFrameUtilities.getAggregatedPurgeMonitoringData(configurationDF, tableName, dataBaseName)
    val monitoringData: DataFrame = DataFrameUtilities.buildMonirtoringRow(principal, PurgedPartitions.toArray, inputRowCount, purgeStartDate, outputRowCount, purgeEndDate, monitoringDatePartition, processingTime, aggregatedMonitoringRow)
    monitoringData
  }

  /**
   * Gets Hive Trigger Table as A DataFrame by aggregating configuration DF and dataBase and table names.
   *
   * @param configurationDF Configuration DF of the Desensitized Table.
   * @param tableName   Desensitized Table Name.
   * @param dataBaseName   Desensitized table DataBase Name.
   * @return monitoring DataFrame that represent aggregated Monitoring data
   */
  def getAggregatedMonitoringData(configurationDF: DataFrame, tableName: String, dataBaseName: String) = {

    logger.info(s"""Aggregate Monitoring Data For the Current Table ${dataBaseName}.${tableName} Desensitization""")
    val STRING_ARRAY = "array<String>"
    val monitoringData = configurationDF.filter(configurationDF(TriggerTableColumn.inputTableName) === tableName && configurationDF(TriggerTableColumn.inputDataBaseName) === dataBaseName)
      .select(TriggerTableColumn.inputDataBaseName,
        TriggerTableColumn.inputTableName,
        TriggerTableColumn.inputColumnName,
        TriggerTableColumn.inputDesensitisationMethod,
        TriggerTableColumn.inputDesensitisationMethodOptions,
        TriggerTableColumn.inputDesensitisationMethodOptionsValues)
      .groupBy(TriggerTableColumn.inputDataBaseName,
        TriggerTableColumn.inputTableName)
      .agg(functions.collect_list(TriggerTableColumn.inputColumnName).cast(STRING_ARRAY),
        functions.collect_list(TriggerTableColumn.inputDesensitisationMethod).cast(STRING_ARRAY),
        functions.collect_list(TriggerTableColumn.inputDesensitisationMethodOptions).cast(STRING_ARRAY),
        functions.collect_list(TriggerTableColumn.inputDesensitisationMethodOptionsValues).cast(STRING_ARRAY)).distinct().persist()
    monitoringData.show()
    monitoringData
  }

  /**
   * Gets Hive Trigger Table as A DataFrame by aggregating configuration DF and dataBase and table names.
   *
   * @param configurationDF Configuration DF of the Desensitized Table.
   * @param tableName   Desensitized Table Name.
   * @param dataBaseName   Desensitized table DataBase Name.
   * @return monitoring DataFrame that represent aggregated Monitoring data
   */
  def getAggregatedPurgeMonitoringData(configurationDF: DataFrame, tableName: String, dataBaseName: String) = {

    logger.info(s"""Aggregate Purge Monitoring Data For the Current Table ${dataBaseName}.${tableName} Desensitization""")
    val STRING_ARRAY = "array<String>"
    val monitoringData = configurationDF.filter(configurationDF(TriggerTableColumn.inputTableName) === tableName && configurationDF(TriggerTableColumn.inputDataBaseName) === dataBaseName)
      .select(TriggerTableColumn.inputDataBaseName,
        TriggerTableColumn.inputTableName,
        TriggerTableColumn.inputColumnName)
      .groupBy(TriggerTableColumn.inputDataBaseName,
      TriggerTableColumn.inputTableName)
      .agg(functions.collect_list(TriggerTableColumn.inputColumnName).cast(STRING_ARRAY)).distinct().persist()
    monitoringData.show()
    monitoringData
  }


  /**
   * Builds Desensitization Monitoring Row after one Table Desensitization.
   *
   * @param principal The user principal who runs the application .
   * @param partitionsToDesensitize List of the desensitized partitions.
   * @param inputRowCount Row count before desensitisation.
   * @param desensitisationStartDate Desensitization start Date.
   * @param outputRowCount Row count after desensitization.
   * @param desensitizationEndDate Desensitization end date.
   * @param monitoringDatePartition Date parition.
   * @param processingTime processing time.
   * @param aggregatedMonitoringRow aggregated rows of the configuration table for specific table.
   * @return monitoringData Row of a Data Frame of the monitoring Data
   */
  def buildMonirtoringRow(principal: String, partitionsToDesensitize: Array[String], inputRowCount: String, desensitisationStartDate: String, outputRowCount: String, desensitizationEndDate: String, monitoringDatePartition: String, processingTime: String, aggregatedMonitoringRow: DataFrame) = {

    logger.info("""Starting to Build Monitoring Row For the Current Desensitization""")
    val monitoringData = aggregatedMonitoringRow
      .withColumn(MonitoringTableColumn.DESENSITIZED_PARTITIONS, functions.lit(partitionsToDesensitize))
      .withColumn(MonitoringTableColumn.INPUT_ROW_COUNT, functions.lit(inputRowCount))
      .withColumn(MonitoringTableColumn.OUTPUT_ROW_COUNT, functions.lit(outputRowCount))
      .withColumn(MonitoringTableColumn.PROCESSING_TIME, functions.lit(processingTime))
      .withColumn(MonitoringTableColumn.START_DATE, functions.lit(desensitisationStartDate))
      .withColumn(MonitoringTableColumn.END_DATE, functions.lit(desensitizationEndDate))
      .withColumn(MonitoringTableColumn.PRINCIPAL_KEYTAB, functions.lit(principal))
      .withColumn(MonitoringTableColumn.DT, functions.lit(monitoringDatePartition)).persist()
    monitoringData
  }


  /**
   * Gets Hive Trigger Table as A DataFrame Used in case if trigger nature is other than execution.
   *
   * @param args Execution Date as a Global Param.
   * @param firstConfigDFRow Table to Desensitized Row of the configuration DF.
   * @return desensitizationTriggerTableDF DataFrame that represents that Desensitized Table
   */
  def getTriggerTableDataFrame(args: Array[String], firstConfigDFRow: Row) : DataFrame = {
    val desensitizationTriggerDataBase = firstConfigDFRow.getAs(TriggerTableColumn.inputTriggerDataBaseName) toString() trim
    val desensitizationTriggerTableName = firstConfigDFRow.getAs(TriggerTableColumn.inputTriggerTableName) toString() trim
    val desensitizationTriggerDataBaseTableName = desensitizationTriggerDataBase.concat(DELIMITER_DOT) concat (desensitizationTriggerTableName)
    val desensitizationTriggerInputPath = firstConfigDFRow.getAs(TriggerTableColumn.inputTriggerTableInputPath) toString() trim
    val desensitizationTriggerInputFormat = firstConfigDFRow.getAs(TriggerTableColumn.inputTriggerTableFormat) toString() trim
    val desensitizationTriggerPartitionColumn = firstConfigDFRow.getAs(TriggerTableColumn.inputTriggerPartitionColumn) toString() trim
    val desensitizationTriggerPartitionColumnValueFormat = firstConfigDFRow.getAs(TriggerTableColumn.inputTriggerPartitionColumnValueFormat) toString() trim
    val desensitizationTableFrequency = firstConfigDFRow.getAs(TriggerTableColumn.inputTriggerTableFrequency) toString() trim
    val triggerTablePartitionsToExtract = desensitizationTableFrequency match {
      case "mois" => DateUtilities.getPartitionsToDesensitize(desensitizationTriggerPartitionColumnValueFormat, DateUtilities.getDesensitizationMonthPartitionValue(args(0).toString, ""))
      case "semaine" => DateUtilities.getTriggerTablePartitions(args(0), desensitizationTriggerPartitionColumnValueFormat, 7)
      case "jours" => DateUtilities.getTriggerTablePartitions(args(0), desensitizationTriggerPartitionColumnValueFormat)
      case _ => logger.warn("Trigger Table Frequency is Missing ! Abborting  Desensitization ...")
    }
    val triggerTablePartitionsToExtractSeq = triggerTablePartitionsToExtract.asInstanceOf[Seq[String]]
    val desensitizationTriggerTableParams = InputIOParams(desensitizationTriggerInputFormat, desensitizationTriggerDataBaseTableName, desensitizationTriggerInputPath, desensitizationTriggerPartitionColumn, DataFrameIOOptions.DEFAULT_DELIMITER, triggerTablePartitionsToExtractSeq)
    val desensitizationTriggerColumnJoin = firstConfigDFRow.getAs(TriggerTableColumn.inputTriggerColumnJoin) toString() trim
    val desensitizationTriggerColumnProcess = firstConfigDFRow.getAs(TriggerTableColumn.inputTriggerColumnProcess) toString() trim
    val desensitizationTriggerColumnProcessValueFormat = firstConfigDFRow.getAs(TriggerTableColumn.inputTriggerColumnProcessValueFormat)
    val columns = Seq(desensitizationTriggerColumnJoin, desensitizationTriggerColumnProcess, desensitizationTriggerColumnProcessValueFormat)
    val desensitizationTriggerColumnProcessMinValue = firstConfigDFRow.getAs(TriggerTableColumn.inputTriggerColumnProcessMinValue) toString() trim
    val desensitizationTriggerTableDataFrame = DataLoader.loadTableWithSpecificColumnAndCondition(desensitizationTriggerTableParams, columns, desensitizationTriggerColumnProcessMinValue, desensitizationTriggerColumnProcess)
    val desensitizationTriggerTableDF = desensitizationTriggerTableDataFrame.withColumn(TriggerTableColumn.inputTriggerColumnProcess, DesensitizationUtilities.reduceProcessColumnValueUDF(desensitizationTriggerTableDataFrame(TriggerTableColumn.inputTriggerColumnProcessValueFormat), desensitizationTriggerTableDataFrame(TriggerTableColumn.inputTriggerColumnProcess)))
    desensitizationTriggerTableDF
  }

  /**
   * Gets Partitions and a DataFrame of the Table to Desensitized used when trigger nature is execution.
   *
   * @param databaseTableName Data Base And Table Name of the Table to Desensitize.
   * @param firstConfigDFRow  First Row of the configuration DF.
   * @param args Execution Date as a Global Param.
   * @return (partitionsToDesensitize, tableToDesensitize) partition to desensitize and table to desensitize as a DataFrame
   */
  def getPartritionsAndTableToDesensitizeMAP(args: Array[String], databaseTableName: String, firstConfigDFRow: Row) = {
    val executionMode = args(2).toString() trim
    val inputPath = firstConfigDFRow.getAs(TriggerTableColumn.inputPathParam).toString() trim
    val inputFormat = firstConfigDFRow.getAs(TriggerTableColumn.inputFormat) toString() trim
    val partitionColumn = firstConfigDFRow.getAs(TriggerTableColumn.inputPartitionColumn) toString() trim
    val partitionValueFormat = firstConfigDFRow.getAs(TriggerTableColumn.inputPartitionValue) toString() trim
    val conservationDuration = firstConfigDFRow.getAs(TriggerTableColumn.inputConservationDuration) toString() trim
    val calculatedPartition = DateUtilities.getDesensitizationMonthPartitionValue(args(0).toString, conservationDuration)
    logger.info(s"the result of applying conservation duration to processing date : ${calculatedPartition} ")
    val partitionsToDesensitize = executionMode match {
      case "stock" => DateUtilities.getStockPartitionsToDesensitize(partitionValueFormat, calculatedPartition)
      case _ =>   DateUtilities.getPartitionsToDesensitize(partitionValueFormat, calculatedPartition)
    }
    logger.info(s"Calculated Partitions to Anonymize  are : ${partitionsToDesensitize}")
    val tableToDesensitizeParams = InputIOParams(inputFormat, databaseTableName, inputPath, partitionColumn, DataFrameIOOptions.DEFAULT_DELIMITER, partitionsToDesensitize)
    val tableToDesensitize = DataLoader.load(tableToDesensitizeParams).persist()
    (partitionsToDesensitize, tableToDesensitize)
  }

  /**
   * Gets Partitions and a DataFrame of the Table to Purge used when trigger nature is execution.
   *
   * @param firstConfigDFRow  First Row of the configuration DF.
   * @param args Execution Date as a Global Param.
   * @return (partitionsToDesensitize, tableToDesensitize) partition to desensitize and table to desensitize as a DataFrame
   */
  def getPartritionsToPurge(args: Array[String], firstConfigDFRow: Row) = {
    val executionMode = args(2).toString() trim
    val partitionValueFormat = firstConfigDFRow.getAs(TriggerTableColumn.inputPartitionValue) toString() trim
    val purgeDuration = firstConfigDFRow.getAs(TriggerTableColumn.inputPurgeDuration) toString() trim
    val calculatedPurgePartition = DateUtilities.getDesensitizationMonthPartitionValue(args(0).toString, purgeDuration)
    logger.info(s"the result of applying purge duration to processing date : ${calculatedPurgePartition} ")
    val partitionsToPurge = executionMode match {
      case "stock" => DateUtilities.getStockPartitionsToDesensitize(partitionValueFormat, calculatedPurgePartition)
      case _ =>   DateUtilities.getPartitionsToDesensitize(partitionValueFormat, calculatedPurgePartition)
    }
    logger.info(s"Calculated Partitions to purge  are : ${partitionsToPurge}")
    (partitionsToPurge)
  }

  /**
   * get database and tableName with Aggregated dataframe of the Configuration data as a tuple from Trigger Table.
   *
   * @param dataBasesAndTablesNames Data Base And Table Name of the Table to Desensitize.
   * @param configurationDF  First Row of the configuration DF.
   * @return configurationMap (databaseTableName, Configuration DataFrame of the concerned Table)
   */
  def getAggregatedConfigurationData(dataBasesAndTablesNames: DataFrame, configurationDF : DataFrame) = {

    var configurationMap = Map[String, DataFrame]()
    dataBasesAndTablesNames.collect().foreach(r => {
      val baseName =  r.get(0)
      val tableName = r.get(1)
      val databaseTableName = baseName + "." + tableName
      val reducedDF = configurationDF.filter(configurationDF(TriggerTableColumn.inputDataBaseName) === baseName &&
        configurationDF(TriggerTableColumn.inputTableName) === tableName)
        .select(TriggerTableColumn.inputColumnName,
          TriggerTableColumn.inputPathParam,
          TriggerTableColumn.inputFormat,
          TriggerTableColumn.inputPartitionColumn,
          TriggerTableColumn.inputPartitionColumnType,
          TriggerTableColumn.inputPartitionValue,
          TriggerTableColumn.outputPath,
          TriggerTableColumn.outputFormat,
          TriggerTableColumn.outputPartitionColumn,
          TriggerTableColumn.inputDesensitisedData,
          TriggerTableColumn.inputConservationDuration,
          TriggerTableColumn.inputPurgeDuration,
          TriggerTableColumn.inputPurgeBoolean,
          TriggerTableColumn.inputDesensitisationMethod,
          TriggerTableColumn.inputDesensitisationMethodOptions,
          TriggerTableColumn.inputDesensitisationMethodOptionsValues,
          TriggerTableColumn.inputTriggerNature,
          TriggerTableColumn.inputTriggerDataBaseName,
          TriggerTableColumn.inputTriggerTableName,
          TriggerTableColumn.inputTriggerTableInputPath,
          TriggerTableColumn.inputTriggerTableFormat,
          TriggerTableColumn.inputTriggerPartitionColumn,
          TriggerTableColumn.inputTriggerPartitionColumnType,
          TriggerTableColumn.inputTriggerPartitionColumnValueFormat,
          TriggerTableColumn.inputTriggerColumnJoin,
          TriggerTableColumn.inputTriggerColumnProcess,
          TriggerTableColumn.inputTriggerColumnProcessValueFormat,
          TriggerTableColumn.inputTriggerTableFrequency,
          TriggerTableColumn.inputTriggerColumnProcessMinValue
        ).persist()
      reducedDF.show()
      configurationMap += (databaseTableName -> reducedDF)
    })
    configurationMap
  }

  def handleDFNullValues(dataFrame :DataFrame, columnToDesensitize : String) : DataFrame = {
    val setNullIfNeeded = {
      udf(f = (value: String) => if (value == "null" || value == "NULL" || value == "Null" || value == null) None else Some(value))
    }
    val inputDataFrameWithNullHandled = dataFrame.withColumn(columnToDesensitize, setNullIfNeeded(col(columnToDesensitize)))
    inputDataFrameWithNullHandled
  }

}
