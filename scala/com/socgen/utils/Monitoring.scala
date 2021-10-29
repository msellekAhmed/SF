package com.socgen.utils

/**
 * Created by X178743 on 14/06/2019.
 * Copyright Société Générale.
 *
 * This Object is used to represent Monitoring Table Columns
 *
 */

abstract class Monitoring

case class MonitoringBean(dataBaseName: String,
                          tableName: String,
                          columnNames: Array[String],
                          desensitizationMethods: Array[String],
                          desensitizationMethodsOptions: Array[String],
                          desensitizationMethodsOptionsValue: Array[String],
                          desensitized_partitions: Array[String],
                          input_row_count: String,
                          output_row_count: String,
                          processing_time: String,
                          start_date: String,
                          end_date: String,
                          principalKeytab: String,
                          dt : String) extends Monitoring


case class PurgeMonitoringBean(dataBaseName: String,
                          tableName: String,
                          columnNames: Array[String],
                          purged_partitions: Array[String],
                          input_row_count: String,
                          output_row_count: String,
                          processing_time: String,
                          start_date: String,
                          end_date: String,
                          principalKeytab: String,
                          dt : String) extends Monitoring