package com.socgen.format.formatters

import com.socgen.format.Formatter
import com.socgen.format.formatters.Handlers.InputHandler
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory


/**
  * Created by X178743 on 14/06/2019.
  * Copyright Société Générale.
  */
/**
  * Allows to create a XmlConf
  * and use a XmlFormatter to load and save Xml files.
  */
object Xml {

  /**
    * Variables (except path) have by default values
    * and can be set to custom values.
    *
    * @constructor : path: String used to load an input or save an output file,
    *              rootTag: String that is tag of XML file to treat as the root,
    *              rowTag: String that is tag of XML file to treat as row,
    *              encoding: String describing charset,
    *              schema: StructType, by default schema is inferred but this parameter allows to use custom schema,
    *              repartition: Int, number of parts to split output DataFrame.
    */
  case class XmlConf(path: String, rootTag: String = "ROWS", rowTag: String = "ROW",
                     encoding: String = "UTF-8", schema: StructType = null, repartition: Int = -1) extends Configuration


  val logger = LoggerFactory.getLogger(classOf[Formatter])

  implicit object XmlFormatter extends InputHandler[XmlConf] {
    /**
      * Takes XML source file pointed by the class parameter conf
      * and converts it into a DataFrame.
      * During conversion the method applies many treatments
      * according to the conf (schema).
      *
      * @param conf XmlConf storing user's choices.
      * @return DataFrame representing data from source file.
      */
    def load(conf: XmlConf)(implicit spark: SparkSession): DataFrame = {
      logger.info(s"Converts a XML file at ${conf.path} into a DataFrame.")
      logger.debug(s"RootTag is ${conf.rootTag} and RowTag is ${conf.rowTag}.")
      logger.debug("Encoding is " + conf.encoding + ".")
      val reader = spark.read
        .format("com.databricks.spark.xml")
        .option("rootTag", conf.rootTag)
        .option("rowTag", conf.rowTag)
        .option("charset", conf.encoding)
      if (conf.schema != null) {
        logger.debug("Uses a custom schema.")
        reader.schema(conf.schema)
      }
      else {
        logger.debug("Infers a schema.")
        reader.option("inferSchema", "true")
      }
      split(reader.load(conf.path), conf.repartition)
    }
  }

}