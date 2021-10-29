package com.socgen.io


/**
 * Created by X178743 on 14/06/2019.
 * Copyright Société Générale.
 *
 * This Object is used to declare different file that we can save in HDFS
 *
 */
object OutputType {
  val CSV = "csv"
  val JSON = "json"
  val ORC = "orc"
  val PARQUET = "parquet"
  val CSV_WITHOUT_HEADER = "csvWithoutHeader"
  val TABLE = "table"
}
