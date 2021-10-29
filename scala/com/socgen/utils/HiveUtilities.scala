package com.socgen.utils

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

/**
 * Created by X178743 on 14/06/2019.
 * Copyright Société Générale.
 *
 * This Object id to implement Hive Utilities Methods
 *
 */

object HiveUtilities {

  private lazy val logger = LogManager.getLogger(this.getClass.getCanonicalName)



  /**
    * This function is used to create a partition in a hive table
    * @param sparkSession : The spark session object to create partitions
    * @param databaseName : The name of the database containing the external tables
    * @param tableName : The name of the Hive Table
    * @param partitionDate : The hive partition date to create
    * @param hdfsOutputPartitionedPath : The output path with the partitionned directory (yyyyMMdd)
    */
  def createHivePartition(sparkSession: SparkSession, databaseName : String, tableName : String, partitionDate : String, hdfsOutputPartitionedPath : String): Unit = {
    logger.info(s"Creating $partitionDate partition for the table $databaseName.$tableName")
    try{
      sparkSession.sql(s"ALTER TABLE $databaseName.$tableName ADD IF NOT EXISTS PARTITION(dt='$partitionDate') LOCATION '$hdfsOutputPartitionedPath'")
    }catch {
      case ex : Exception => logger.error(s"An Exception occured whend creating partiton (dt=$partitionDate) for the table $tableName :\n${ex.printStackTrace()}")
    }
  }

  /**
   * This function is used to drop a partition in a hive table
   * @param database : The database of the concerned table
   * @param table : The name of the Hive Table
   * @param partitionColumn : The hive table partition column
   * @param partitionValue : The hive partition date to drop
   */
  def deletePartitionQuery (databaseTableName: String, partitionColumn: String, partitionValue: String ) : String = {
    s"Alter table $databaseTableName drop if exists partition ($partitionColumn='$partitionValue')"
  }


}
