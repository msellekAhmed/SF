package com.socgen.utils

import java.text.{DateFormat, SimpleDateFormat}
import java.util.{Calendar, Date}
import java.util.logging.Logger
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.apache.spark.sql.DataFrame
import org.joda.time.{DateTime, Days, Months}

/**
 * Created by X178743 on 14/06/2019.
 * Copyright Société Générale.
 *
 * This Object id to implement different time and Date computations used for calculating partitions to desensitize
 *
 */

object DateUtilities {

  val logger = Logger.getLogger(this.getClass.getName)
  val DELIMITER_DOT = "."
  val MONITORING_DATE_FORMAT = "dd/MM/yyyy hh:mm:ss" + DELIMITER_DOT + "SSS"
  val MONITORING_DATE_PARTITION_FORMAT = "yyyy-MM-dd"


  /**
    * Format a Given Date in MS to A Given Date Format
   *  @param dateFormat output date format
   *  @param partitionValue Date value
   *  @return dateValue the desired date format
    */
  def formatDateTime(dateFormat : String, partitionValue : Long): String ={
    var formatter = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS")
    dateFormat match {
      case "yyyy-MM-dd" =>  formatter = new SimpleDateFormat("yyyy-MM-dd")
      case "yyyyMMdd" =>  formatter = new SimpleDateFormat("yyyyMMdd")
      case _ => formatter =  new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS")
    }
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(partitionValue)
    val dateValue = formatter.format(calendar.getTime)
    dateValue
  }

  /**
   * Return a sequence of trigger dates depending on a trigger event other than the execution date with different frequency
   *  @param desensitizationDate Date of the
   *  @param partitionValueFormat Date value
   *  @param daysToSubtract Date value
   *  @return substractedDates the date after substruction
   */
  def getTriggerTablePartitions(desensitizationDate : String, partitionValueFormat : String, daysToSubtract : Int = 1): Seq[String] = {
    val dateFormatter: DateFormat = new SimpleDateFormat(partitionValueFormat)
    val currentDate: Date = dateFormatter.parse(desensitizationDate)
    val calendar = Calendar.getInstance
    calendar.setTime(currentDate)
    val subtractedDates = for ( i <- 1 to daysToSubtract ) yield {
      calendar.add(Calendar.DATE, -1)
      dateFormatter.format(calendar.getTime)
    }
    subtractedDates
  }

  /**
    * Apply Conservation Duration to the Execution Date to get a Global Year and Month Desensitization Partition
   * (we use Month because conservation duration is in Month)
   *
   * @param  desensitizationDate Execution Date
   * @param conservationDuration Conservation Duration
   * @return dateValue Return the base Date With YEar And Mont after substracting the conservation duration
    */
  def getDesensitizationMonthPartitionValue(desensitizationDate : String, conservationDuration : String ): String ={

    val dateFormat: DateFormat = new SimpleDateFormat("yyyyMMdd")

    logger.info(s"Duration Param is ${conservationDuration} Month And trigger Start date is : ${desensitizationDate}")
    val durationToSubtract = Integer.parseInt(conservationDuration)
    val currentDate: Date = dateFormat.parse(desensitizationDate)
    val cal = Calendar.getInstance
    cal.setTime(currentDate)
    cal.add(Calendar.MONTH, -durationToSubtract)
    val dateValue = dateFormat.format(cal.getTime())
    logger.info(s"Global Year Month Desensitization partition is ${dateValue}")
    dateValue
  }

  /**
   * get the days in a month partitions to Desensitize starting from a Global YEAR and MONTH and DAY provided Date
   *
   * @param partitionValueFormat partition value Format
   * @param partition Partition Value
   */
  private def getPartitionsValuesToDesensitize(partitionValueFormat : String, partition : String) : Seq[String] = {

    val formatter = new SimpleDateFormat(partitionValueFormat)
    val calendar = Calendar.getInstance
    val monthPartition = partition.filterNot("-".toSet).substring(4,6)
    val yearPartition = partition.filterNot("-".toSet).substring(0,4)
    calendar.set(yearPartition.toInt, monthPartition.toInt, 0)
    calendar.add(Calendar.MONTH, -1)
    val daysInMonth = calendar.getActualMaximum(Calendar.DAY_OF_MONTH)
    (1 to daysInMonth).map(_ => {
      calendar.roll(Calendar.DATE, true)
      formatter.format(calendar.getTime)
    })
  }

  /**
   * get the months  partitions to Desensitize starting from a Global YEAR and MONTH provided Date
   *
   * @param partitionValueFormat partition value Format
   * @param partition Partition Value
   */
  private def getYearMonthPartitionsValuesToDesensitize(partitionValueFormat : String, partition : String) : Seq[String] = {

    val formatter = new SimpleDateFormat(partitionValueFormat)
    val calendar = Calendar.getInstance
    val monthPartition = partition.filterNot("-".toSet).substring(4,6)
    val yearPartition = partition.filterNot("-".toSet).substring(0,4)
    calendar.set(yearPartition.toInt, monthPartition.toInt, 0)
    calendar.add(Calendar.MONTH, -1)
    Seq(formatter.format(calendar.getTime))
  }


  /**
   * get the days in a month partitions to Desensitize starting from a Global YEAR and MONTH provided Date
   *
   * @param partitionValueFormat partition value Format
   * @param partition Partition Value
   */
  def getStockPartitionsValuesToDesensitize(partitionValueFormat : String, partition : String, minDate : String) : Seq[String] = {

    val jodaTimeFormatters = DateTimeFormat.forPattern(partitionValueFormat)

    val startDate = jodaTimeFormatters.parseDateTime(minDate)
    val endDate = jodaTimeFormatters.parseDateTime(partition)
    val DatesArray =  partitionValueFormat match {
      case "yyyyMMdd" => getDaysBetween(startDate, endDate)
      case "yyyyMM" => getMonthsBetween(startDate,  endDate)
      case "yyyy-MM-dd" => getDaysBetween(startDate, endDate)
      case "yyyy-MM" => getMonthsBetween(startDate,  endDate)
    }
    DatesArray.map(_.toString(jodaTimeFormatters))
  }

  private def getDaysBetween(startDate: DateTime, endDate: DateTime) = {
    val daysCount = Days.daysBetween(startDate, endDate).getDays
    (0 until daysCount).map(startDate.plusDays(_))
  }

  private def getMonthsBetween(startDate: DateTime, endDate: DateTime) = {
    val monthsCount = Months.monthsBetween(startDate,  endDate).getMonths
    (0 until monthsCount).map(startDate.plusMonths(_))
  }
  /**
    * Depending of Date Format calls the specific method to process partitions to Desensitize
   *
   * @param partitionFormat partition value Format
   * @param partitionValue Partition Value
    */
  def getPartitionsToDesensitize(partitionFormat: String, partitionValue : String): Seq[String] = partitionFormat match {

    case "yyyyMMdd" => getPartitionsValuesToDesensitize(partitionFormat,partitionValue)
    case "yyyyMM" => getYearMonthPartitionsValuesToDesensitize(partitionFormat, partitionValue)
    case "yyyy-MM-dd" => getPartitionsValuesToDesensitize(partitionFormat, partitionValue)
    case "yyyy-MM" => getYearMonthPartitionsValuesToDesensitize(partitionFormat, partitionValue)
    case _ => Seq("none")
  }

  /**
   * Depending of Date Format calls the specific method to process partitions to Desensitize in Stock Execution Mode
   *
   * @param partitionFormat partition value Format
   * @param partitionValue Partition Value
   */
  def getStockPartitionsToDesensitize(partitionFormat: String, partitionValue : String): Seq[String] ={
    partitionFormat match {
    case "yyyyMMdd" => getStockPartitionsValuesToDesensitize(partitionFormat,partitionValue, "20130101")
    case "yyyyMM" => getStockPartitionsValuesToDesensitize(partitionFormat, partitionValue, "201301")
    case "yyyy-MM-dd" => {
      getStockPartitionsValuesToDesensitize(partitionFormat, partitionValue.patch(4, "-", 0).patch(7, "-", 0), "2013-01-01")
    }
    case "yyyy-MM" => getStockPartitionsValuesToDesensitize(partitionFormat, partitionValue.patch(4, "-", 0), "2013-01")
    case _ => Seq("none")}
  }


}
