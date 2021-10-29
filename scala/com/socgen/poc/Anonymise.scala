package com.socgen.poc


import com.socgen.format.options.DefaultValue
import com.socgen.io._
import com.socgen.sdt.cleaning.hive.HiveUtils
import com.socgen.utils.DataFrameUtilities.MONITORING_DATE_FORMAT
import com.socgen.utils.DesensitizationUtilities.logger
import com.socgen.utils._
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{types, _}
import org.apache.log4j.LogManager
import sun.management.snmp.AdaptorBootstrap.DefaultValues

import scala.collection.Seq


object Anonymise {

  private lazy val logger = LogManager.getLogger(this.getClass.getName)
  private val YARN_CLUSTER = "yarn-cluster"
  private val DESENSITIZATION_ENGINE = "Moteur de conservation GDPR"
  private val triggerTableParamsFileName = "trigger"
  private val monitoringTableParamsFileName = "monitoring"
  private val purgeTableParamsFileName = "purge"

  /**
    * Building the sparkSession using the provided configuration
    */
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master(YARN_CLUSTER)
    .appName(DESENSITIZATION_ENGINE)
    .config("spark" + DateUtilities.DELIMITER_DOT + "serializer", "org" + DateUtilities.DELIMITER_DOT + "apache" + DateUtilities.DELIMITER_DOT + "spark" + DateUtilities.DELIMITER_DOT + "serializer" + DateUtilities.DELIMITER_DOT + "KryoSerializer")
    .config("spark" + DateUtilities.DELIMITER_DOT + "scheduler" + DateUtilities.DELIMITER_DOT + "mode", "FAIR")
    .config("","")
    .enableHiveSupport()
    .getOrCreate()



  def main(args: Array[String]) {

    import spark.implicits._

    var monitoringDF = Seq.empty[MonitoringBean].toDF()
    var purgeMonitoringDF = Seq.empty[PurgeMonitoringBean].toDF()
    val principal = UserGroupInformation.getCurrentUser.getUserName
    logger.info(s"Principal User to Execute the Job is : ${principal}")

    logger.info("*********************************************************************************** Loading Configuration Table Data *****************************************************************************************************")
    val configurationTablePartitionValue = Seq(args(0).toString.substring(0, 6))
    logger.info(s"Configuration Table Partition to Process is : ${configurationTablePartitionValue}")
    logger.info(s"Getting Configuration Table Params ...")

    val (format, dataBaseName,tableName, inputPath, inputPathWithDt, partitionColumn, defaultDelimiter) = TableParamsExtractor.getTableParams(triggerTableParamsFileName)
    val databaseTableName = dataBaseName.concat(DateUtilities.DELIMITER_DOT).concat(tableName)
    val configurationTableInputParams = InputIOParams(format, databaseTableName, inputPath, partitionColumn, defaultDelimiter, configurationTablePartitionValue)
    val configurationDF = DataLoader.load(configurationTableInputParams).persist()
    configurationDF.show()

    logger.info("********************************************************************** Getting DataBase and Table Name of the data to desensitize *****************************************************************************************************")
    val dataBasesAndTablesNamesDF = configurationDF.select(TriggerTableColumn.inputDataBaseName, TriggerTableColumn.inputTableName).distinct()
    dataBasesAndTablesNamesDF.show()

    logger.info("************************************************************************* Getting Aggregated Configuration Table Data *****************************************************************************************************")
    val aggregatedConfigurationDataMAP: Map[String, DataFrame] = DataFrameUtilities.getAggregatedConfigurationData(dataBasesAndTablesNamesDF, configurationDF)

    logger.info("*********************************************************************************** Starting Desensitization *****************************************************************************************************")

    aggregatedConfigurationDataMAP foreach (
      (tuple)  => {
        val databaseTableName = tuple._1
        val configDF = tuple._2
        val firstConfigDFRow = configDF.first()
        val conservationDuration = firstConfigDFRow.getAs(TriggerTableColumn.inputConservationDuration) toString() trim
        val triggerNature = firstConfigDFRow.getAs(TriggerTableColumn.inputTriggerNature) toString() trim
        val purgeBoolean = firstConfigDFRow.getAs(TriggerTableColumn.inputPurgeBoolean) toString() trim

        val outPut = triggerNature match {
          case "execution" => {
            logger.info("Desensitization Trigger Nature Was Set To Execution Date ! Processing ...")
           //val partitionsAndTableToDesensitize = DataFrameUtilities.getPartritionsAndTableToDesensitizeMAP(args, databaseTableName, firstConfigDFRow)
           //val partitionsToDesensitize = partitionsAndTableToDesensitize._1
           //val tableToDesensitize = partitionsAndTableToDesensitize._2
           val partitionsToPurge = DataFrameUtilities.getPartritionsToPurge(args, firstConfigDFRow)
           //tableToDesensitize.persist()
           //val inputRowCount = tableToDesensitize.count().toString
           //val desensitisationStartMillisecond = System.currentTimeMillis()
           //val desensitisationStartDate = DateUtilities.formatDateTime(DateUtilities.MONITORING_DATE_FORMAT, desensitisationStartMillisecond)
           //logger.info(s"Starting Desensitization of ${databaseTableName} with Partitions : ${partitionsToDesensitize}:")
           //val desensitizedData = DesensitizationUtilities.desensitizeTableColumns(configDF, tableToDesensitize).persist()
           //logger.info("Showing the Data Frame of the Desensitized data :")
           //desensitizedData.show()
           //val outputRowCount = desensitizedData.count()
           //logger.info("****************************************************************************** Saving Desensitized Data *****************************************************************************************************")
           //DesensitizationUtilities.saveDesensitizedData(databaseTableName, firstConfigDFRow, partitionsToDesensitize, desensitizedData)
           //val monitoringData: DataFrame = DataFrameUtilities.getMonitoringDataRowDF(principal, configurationDF, databaseTableName, partitionsToDesensitize, inputRowCount, desensitisationStartMillisecond, desensitisationStartDate, desensitizedData)
           //monitoringDF = monitoringDF.union(monitoringData)
            if (purgeBoolean equals "true")
              {
                logger.info("****************************************************************************** Starting purging original Data *****************************************************************************************************")
                logger.info(s"${databaseTableName} partitions to purge are : ${partitionsToPurge}")
                val purgeStartMillisecond = System.currentTimeMillis()
                val purgeStartDate = DateUtilities.formatDateTime(DateUtilities.MONITORING_DATE_FORMAT, purgeStartMillisecond)
                partitionsToPurge.map(partitionToPurge => HiveUtils.deletePartition(databaseTableName.split("\\" + ".")(0), databaseTableName.split("\\" + ".")(1), DefaultValue.PARTITION_COLUMN, partitionToPurge))
                val purgeEndMillisecond = System.currentTimeMillis()
                val purgeEndDate = DateUtilities.formatDateTime(DateUtilities.MONITORING_DATE_FORMAT, purgeEndMillisecond)
                val purgeMonitoringData = DataFrameUtilities.getPurgeMonitoringDataRowDF(principal, configurationDF, databaseTableName, partitionsToPurge, "", purgeStartMillisecond, purgeStartDate, purgeEndMillisecond, purgeEndDate, "".toString )
                purgeMonitoringData.show()
                purgeMonitoringDF = purgeMonitoringDF.union(purgeMonitoringData)
                purgeMonitoringDF.show()
              }
          }
          case "" => {
            logger.info("No Desensitization Trigger Nature Has been Provided, Please Provide a Desensitization Trigger. Shutting Down The App !")
            spark.stop()
          }
          case _ => {
            logger.info(s"Desensitization Trigger Nature Was Set To ${triggerNature} ! Processing ...")
            val desensitizationTriggerPartitionColumnValueFormat = firstConfigDFRow.getAs(TriggerTableColumn.inputTriggerPartitionColumnValueFormat) toString() trim
            val desensitizationTriggerJoinColumn = firstConfigDFRow.getAs(TriggerTableColumn.inputTriggerColumnJoin) toString() trim
            val desensitizationTriggerProcessColumn = firstConfigDFRow.getAs(TriggerTableColumn.inputTriggerColumnProcess) toString() trim
            val triggerTableDF = DataFrameUtilities.getTriggerTableDataFrame(args, firstConfigDFRow)
            val triggerTablePartitions = DesensitizationUtilities.getTriggerPartitions(triggerTableDF)
            triggerTablePartitions foreach( partition => {
              val triggerTablePartitionToDesensitize = DesensitizationUtilities.getPartitionsToDesensitize(partition, desensitizationTriggerPartitionColumnValueFormat, args(0), conservationDuration, "yyyyMM")
              val (ids, partitions) = DesensitizationUtilities.getIdsAndPartitionsToDesensitize(databaseTableName, DataFrameIOOptions.DEFAULT_PARTITION_COLUMN, triggerTableDF, desensitizationTriggerProcessColumn, desensitizationTriggerJoinColumn, triggerTablePartitionToDesensitize)
              val (dataToDesensitize, dataToNotDesensitize) = DesensitizationUtilities.getDataToDesensitize((ids, partitions), databaseTableName, firstConfigDFRow)
              val inputRowCount = dataToDesensitize.count().toString
              val desensitisationStartMillisecond = System.currentTimeMillis()
              val desensitisationStartDate = DateUtilities.formatDateTime(DateUtilities.MONITORING_DATE_FORMAT, desensitisationStartMillisecond)
              val desensitizedData = DesensitizationUtilities.desensitizeTableColumns(configDF, dataToDesensitize).persist()
              logger.info("Showing the Data Frame of the Anonymized data :")
              desensitizedData.show()
              val globalData = desensitizedData.union(dataToNotDesensitize)
              logger.info("****************************************************************************** Saving Desensitized Data *****************************************************************************************************")
              DesensitizationUtilities.saveDesensitizedData(databaseTableName, firstConfigDFRow, partitions, globalData)
              val monitoringData: DataFrame = DataFrameUtilities.getMonitoringDataRowDF(principal, configurationDF, databaseTableName, partitions, inputRowCount, desensitisationStartMillisecond, desensitisationStartDate, desensitizedData)
              monitoringDF = monitoringDF.union(monitoringData)
            })
          }
        }})

      logger.info("******************************************************************************** Storing Monitoring Info *****************************************************************************************************")
      val monitoringDatePartition = DateUtilities.formatDateTime(DateUtilities.MONITORING_DATE_PARTITION_FORMAT, System.currentTimeMillis())
    //val (tableFormat, monitoringDataBaseName, monitoringTableName, monitoringInputPath, monitoringInputPathWithDt, monitoringPartitionColumn, monitoringDefaultDelimiter) = TableParamsExtractor.getTableParams(monitoringTableParamsFileName)
    //val monitoringTableParams = OutputIOParams(tableFormat, monitoringTableName, monitoringInputPath, monitoringPartitionColumn, monitoringDefaultDelimiter, Seq(monitoringDatePartition))
    //DataWriter.save(monitoringDF, monitoringTableParams)
    //val monitoringHDFSPartitionPath = monitoringInputPathWithDt.concat("dt=").concat(monitoringDatePartition)
    //HiveUtilities.createHivePartition(spark, monitoringDataBaseName, monitoringTableName, monitoringDatePartition, monitoringHDFSPartitionPath)

    val (purgeTableFormat, purgeDataBaseName, purgeTableName, purgeInputPath, purgeInputPathWithDt, purgePartitionColumn, purgeDefaultDelimiter) = TableParamsExtractor.getTableParams(purgeTableParamsFileName)
    val purgeTableParams = OutputIOParams(purgeTableFormat, purgeTableName, purgeInputPath, purgePartitionColumn, purgeDefaultDelimiter, Seq(monitoringDatePartition))
    DataWriter.save(purgeMonitoringDF, purgeTableParams)
    val purgeHDFSPartitionPath = purgeInputPathWithDt.concat("dt=").concat(monitoringDatePartition)
    HiveUtilities.createHivePartition(spark, purgeDataBaseName, purgeTableName, monitoringDatePartition, purgeHDFSPartitionPath)

    spark.stop
  }



}