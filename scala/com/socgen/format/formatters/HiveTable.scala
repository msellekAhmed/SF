package com.socgen.format.formatters

import com.socgen.format.Formatter
import com.socgen.format.formatters.Handlers.{InputHandler, OutputHandler}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object HiveTable {

  private val EMPTY_STRING = ""
  private val ORC_FORMAT = "orc"
  private val OPTION_PATH = "path"

  /**
    * Variables (except tableName) have by default values
    * and can be set to custom values.
    *
    * @constructor : tableName : String corresponding to table's name,
    *              path : String corresponding to path for saving table data (by default it is current folder),
    *              partitionedBy : String, name of the column used to partition loaded Hive table,
    *              partitions : List[String] corresponding to expected partition's values (cannot be empty if partitionedBy is defined),
    *              repartition: Int, number of parts to split output DataFrame.
    */
  case class HiveTableConf(tableName: String, path: String = ".",
                           partitionedBy: String = EMPTY_STRING, partitions: Seq[String] = Nil,
                           repartition: Int = -1) extends Configuration

  val logger = LoggerFactory.getLogger(classOf[Formatter])

  implicit object HiveTableFormatter extends InputHandler[HiveTableConf] with OutputHandler[HiveTableConf] {


    /**
      * Takes Hive table Columns pointed by class parameter conf
      * and converts it into DataFrame.
      * During conversion method applies many treatments
      * according to conf (repartition).
      *
      * @param conf  HiveTableConf storing user's choices.
      * @param spark SparkSession as implicit parameter.
      * @return DataFrame representing data from source table.
      */
    def loadSpecificColumnAndCondition(conf: HiveTableConf, columns : Seq[String], minimalValue : String, conditionColumn : String)(implicit spark: SparkSession): DataFrame = {
      val dataFrame = conf.partitionedBy match {
        case EMPTY_STRING =>
          logger.info(s"Load Hive table ${conf.tableName} With Specific Columns ${columns}.")
          spark.table(conf.tableName)

        case _ =>
          logger.info(s"Load Hive table ${conf.tableName} partitioned by ${conf.partitionedBy} With Specific Columns ${columns}..")
          val maybeQuery = getLoadPartitionsSpecificColumnQueryAndCondition(conf.tableName, conf.partitionedBy, conf.partitions, columns, minimalValue, conditionColumn)
          maybeQuery match {
            case Success((query, andCondition)) => { minimalValue match {
              case "" => spark.sql(query)
              case _ => spark.sql(query + andCondition)
            }  }
            case Failure(error) => throw error
          }
      }

      split(dataFrame, conf.repartition)
    }


    private def getLoadPartitionsSpecificColumnQueryAndCondition(tableName: String, columnName: String, partitions: Seq[String], columns : Seq[String], minimalValue : String, conditionColumn : String ): Try[(String, String)] = {
      (partitions, columns) match {

        case (firstPartition +: nextPartitions, firstColumn +: nextColumns) =>
          val select = nextColumns.foldLeft(s"SELECT $firstColumn") {
            (query, column) => query + s" , $column "
          }
          val from = s"FROM $tableName"
          val where = nextPartitions.foldLeft(s"WHERE ( $columnName='$firstPartition'") {
            (query, partition) => query + s" OR $columnName='$partition'"
          }
          val parenthese = s")"
          val and = s" AND ("
          val condition = s" $conditionColumn >= '$minimalValue' )"
          Success((s"$select $from $where $parenthese",s"$and $condition"))

        case (Nil, firstColumn +: nextColumn) =>
          Failure(new IllegalArgumentException("You must define partitions argument : "))

        case (firstPartition +: nextPartitions, Nil)  =>
          Failure(new IllegalArgumentException("You must define Columns argument : " ))

        case (Nil, Nil)  =>
          Failure(new IllegalArgumentException("You must define partitions and Columns argument : "))

      }
    }


    /**
      * Takes Hive table pointed by class parameter conf
      * and converts it into DataFrame.
      * During conversion method applies many treatments
      * according to conf (repartition).
      *
      * @param conf  HiveTableConf storing user's choices.
      * @param spark SparkSession as implicit parameter.
      * @return DataFrame representing data from source table.
      */
    override def load(conf: HiveTableConf)(implicit spark: SparkSession): DataFrame = {
      val dataFrame = conf.partitionedBy match {
        case EMPTY_STRING =>
          logger.info(s"Load Hive table ${conf.tableName}.")
          spark.table(conf.tableName)

        case _ =>
          logger.info(s"Load Hive table ${conf.tableName} partitioned by ${conf.partitionedBy}.")
          val maybeQuery = getLoadPartitionsQuery(conf.tableName, conf.partitionedBy, conf.partitions)
          logger.info(s"***********************************************************Query to Execute : ${maybeQuery}  ***********************************************************************************")
          maybeQuery match {
            case Success(query) => spark.sql(query)
            case Failure(error) => throw error
          }
      }

      split(dataFrame, conf.repartition)
    }

    private def getLoadPartitionsQuery(tableName: String, columnName: String, partitions: Seq[String]): Try[String] = {
      partitions match {

        case firstPartition +: nextPartitions =>
          val select = s"SELECT * FROM $tableName"
          val where = nextPartitions.foldLeft(s"WHERE $columnName='$firstPartition'") {
            (query, partition) => query + s" OR $columnName='$partition'"
          }
          Success(s"$select $where")

        case Nil =>
          Failure(new IllegalArgumentException("You must define partitions argument : " +
            "partitionedBy argument is defined but partitions argument is empty."))


      }
    }

    /**
      * Takes a DataFrame and saves it into a Hive table
      * according to name from conf parameter.
      * It uses path from HiveTableConf and saves DataFrame as an ORC file.
      *
      * @param conf HiveTableConf storing user's choices.
      * @param df   DataFrame representing data to save.
      */
    override def save(conf: HiveTableConf, df: DataFrame): Unit = {
      logger.info(s"Save DataFrame into ${conf.tableName} Hive table at path ${conf.path}.")
      df.write
        .format(ORC_FORMAT)
        .option(OPTION_PATH, conf.path)
        .mode(SaveMode.Append)
        .saveAsTable(conf.tableName)
    }

  }

}
