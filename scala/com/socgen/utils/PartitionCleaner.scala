package com.socgen.utils

import com.socgen.io.InputIOParams
import com.socgen.sdt.logging.Logging
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object PartitionCleaner extends Logging  {

  def cleanPartition(tableName: String, partitionColumn: String, partitionValues: Seq[String])(implicit spark: SparkSession): Try[Unit] = {
    logger.info("Starting cleaning partitions...")
    val partitionsList = partitionValues
    val partitionDeletionResults = partitionsList.map(partition => deletePartition(tableName, partitionColumn, partition))

    partitionDeletionResults.foreach(_ match {
      case Failure(exception) => logger.error("Could not drop partition", exception )
      case _ =>
    })

    logger.info("Cleaning finished.")
    val partitionResultsFailure = partitionDeletionResults.filter(_.isFailure)
    if (partitionResultsFailure.nonEmpty) Failure(new PartitionException("Could not quarantine the following file" + partitionResultsFailure))
    else Success()
  }

  def deletePartition(database: String, partitionColumn: String, partition: String)(implicit spark: SparkSession): Try[Unit] = {
    val queryDeletePartition = HiveUtilities.deletePartitionQuery(database, partitionColumn, partition)

    Try(spark.sql(queryDeletePartition)) match {
      case Success(_) => Success(Unit)
      case Failure(e) => Failure(new Exception("Could not execute delete partition query please check your configurqtion or qrgument"))
    }
  }

}
