package com.socgen.format.formatters

import com.socgen.format.Formatter
import com.socgen.format.formatters.Handlers.InputHandler
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory


/**
  * Created by X178743 on 14/06/2019.
  * Copyright Société Générale.
  */
/**
  * Allows to create a PositionalConf
  * and use a PositionalFormatter to load and save Positional files.
  */

object Positional {

  /**
    * Variables (except path and blocks) have by default values
    * and can be set to custom values.
    *
    * @constructor : path: String used to load an input or save an output file,
    *              blocks: List of Int representing blocks' sizes,
    *              useHeader: Boolean, when is true first line of file is used to name columns and is not part of data,
    *              schema: StructType, by default schema is inferred but this parameter allows to use custom schema,
    *              repartition: Int, number of parts to split output DataFrame.
    */
  case class PositionalConf(path: String, blocks: List[Int], useHeader: Boolean = true, schema: StructType = null, repartition: Int = -1) extends Configuration

  val logger = LoggerFactory.getLogger(classOf[Formatter])

  implicit object PositionalFormatter extends InputHandler[PositionalConf] {
    /**
      * Takes Positional source file pointed by class parameter conf
      * and converts it into a DataFrame.
      * During conversion method applies many treatments
      * according to conf (header, schema).
      *
      * @param conf PositionalConf storing user's choices.
      * @return DataFrame representing data from source file.
      */
    def load(conf: PositionalConf)(implicit spark: SparkSession): DataFrame = {
      import PositionalParser._
      logger.info("Converts Positional file at " + conf.path + " into a DataFrame.")
      logger.debug("Uses a header = " + conf.useHeader.toString + ".")
      logger.debug("Blocks sizes are = " + conf.blocks.toString())
      val blocks = generateTuples(conf.blocks)
      var schemaString = ""

      // Creates a RDD using source file.
      var rdd = spark.read.textFile(conf.path).rdd
        .map(line => getRow(line, blocks))

      // Takes and removes header if useHeader from conf is true.
      if (conf.useHeader) {
        val header = rdd.first()
        schemaString = header.mkString(" ")
        rdd = rdd.mapPartitionsWithIndex { (idx, iterator) => if (idx == 0) iterator.drop(1) else iterator }
      }
      if (schemaString.isEmpty) {
        schemaString = getByDefaultNames(conf.blocks)
      }

      // Creates a schema.
      val schema = createSchema(conf.blocks, schemaString)

      // Creates a DataFrame.
      var df = spark.createDataFrame(rdd, schema)

      // Applies custom schema if desired.
      if (conf.schema != null) {
        df = df.toDF(conf.schema.fieldNames: _*)
        for (i <- conf.schema.iterator) {
          df(i.name).cast(i.dataType)
        }
      }

      split(df, conf.repartition)
    }
  }

  /*
Parses lines in positional format and creates schema
to transforms a RDD into a DataFrame.
*/
  object PositionalParser {
    /* According to blocks' size generates a List of tuples (Int, Int)
    to describes first and last position of blocks.
     */
    def generateTuples(blocks: List[Int]): List[(Int, Int)] = {
      (blocks foldLeft List[(Int, Int)]()) {
        case (List(), block) => List((0, block))
        case (acc, block) => acc :+ (acc.last._2, acc.last._2 + block)
      }
    }

    /*
    Returns Row parsing a line according to blocks sizes.
    */
    def getRow(line: String, blocks: List[(Int, Int)]): Row = {
      Row(blocks.map(block => line.substring(block._1, block._2).trim()): _*)
    }

    /* Returns a list of by default column names.
    */
    def getByDefaultNames(blocks: List[Int]): String = {
      (blocks.indices foldLeft "") {
        (acc, index) => acc.concat("C" + index + " ")
      }.trim
    }

    /*
    Returns a schema using header or names by default.
    */
    def createSchema(blocks: List[Int], schemaString: String): StructType = {
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType))
      )
    }
  }

}