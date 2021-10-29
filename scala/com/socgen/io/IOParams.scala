package com.socgen.io

/**
 * Created by X178743 on 14/06/2019.
 * Copyright Société Générale.
 *
 * This interface is used to declare different Input and Output params so that we can load and save different file and
 * data in HDFS
 *
 */
abstract class IOParams

case class InputIOParams(format : String, tableName : String, path : String, partitionColumn : String, delimiter : String, partitionValues : Seq[String]) extends IOParams
case class OutputIOParams(format : String, tableName : String, path : String, partitionColumn : String, delimiter : String, partitionValues : Seq[String]) extends IOParams
