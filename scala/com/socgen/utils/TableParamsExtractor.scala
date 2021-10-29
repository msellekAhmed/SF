package com.socgen.utils

import java.io.InputStreamReader
import java.util.Properties

import com.typesafe.config._
import org.apache.log4j.LogManager

/**
 * Created by X178743 on 14/06/2019.
 * Copyright Société Générale.
 *
 * This Object id to extract variable values from properties files
 *
 */

object TableParamsExtractor extends Serializable {


  private lazy val logger = LogManager.getLogger(this.getClass.getCanonicalName)
  /**
    * This function is used to get Trigger Table and Monitoring Table Params
    * @param table properties file name
    * @return (format :String, tableName :String, InputPath :String, partitionColumn :String, defaultDelimiter :String)
    */
  def getTableParams(table : String): (String, String, String, String, String, String ,String) ={

    val config : Config = ConfigFactory.load("lightbend.conf")

    val format = config.getString(s"$table.format")
    val tableName = config.getString(s"$table.tableName")
    val databaseName = config.getString(s"$table.dataBaseName")
    val inputPath = config.getString(s"$table.inputPath")
    val inputPathWithDt = config.getString(s"$table.inputPathWithDt")
    val partitionColumn = config.getString(s"$table.partitionColumn")
    val defaultDelimiter = config.getString(s"$table.defaultDelimiter")

    logger.info(s"Params extracted are : \nformat: $format, tableName: $tableName, dataBaseName: $databaseName, inputPath: $inputPath, inputPathWithDt: $inputPathWithDt, partitionColumn: $partitionColumn, defaultDelimiter: $defaultDelimiter ")
    (format, databaseName, tableName, inputPath, inputPathWithDt, partitionColumn, defaultDelimiter)
  }


  /**
   * This function is used to get kmip properties for CASA Encryption
   * @return (keyName :String, keyVersion :String, keySecureServerURL :String, keySecureServerPort :String, localKeystorePath :String)
   */
  def getKmipPropertiesParams() : (String, String, String, String, String, String, String, String, String ) = {

    val config : Config = ConfigFactory.load("lightbend.conf")

    val keyName = config.getString("casa.fpeKeyName")
    val keyVersion = config.getString("casa.fpeKeyVersion")
    val keySecureServerURL = config.getString("casa.keysecureServerUrl")
    val keySecureServerPort = config.getString("casa.keysecureServerPortDev")
    val localKeystorePath = config.getString("casa.localKeystorePath")
    val keysecureUsername = config.getString("casa.keysecureUsername")
    val keysecureUserPassword = config.getString("casa.keysecureUserPassword")
    val localKeystoreAlias = config.getString("casa.localKeystoreAlias")
    val localKeystorePassword = config.getString("casa.localKeystorePassword")

    logger.info(s"Params extracted are : \nkeyName: $keyName keyVersion:$keyVersion, keySecureServerURL: $keySecureServerURL, keySecureServerPort: $keySecureServerPort, localKeystorePath: $localKeystorePath, keysecureUsername: $keysecureUsername, keysecureUserPassword: $keysecureUserPassword, localKeystoreAlias: $localKeystoreAlias, localKeystorePassword: $localKeystorePassword ")
    (keyName, keyVersion, keySecureServerURL, keySecureServerPort, localKeystorePath, keysecureUsername, keysecureUserPassword, localKeystoreAlias,localKeystorePassword)

  }

  /**
   * This function is used to get properties for Hadoop Credential API Read
   * @return (keyName :String, keyVersion :String, keySecureServerURL :String, keySecureServerPort :String, localKeystorePath :String)
   */
  def getCredentialPropertiesParams() : ( String, String, String, String, String ) = {

    val config : Config = ConfigFactory.load("lightbend.conf")

    val keySecureLoginAlias = config.getString("credential.keySecureLoginAlias")
    val keySecurePasswdAlias = config.getString("credential.keySecurePasswdAlias")
    val localKeystoreLoginAlias = config.getString("credential.localKeystoreLoginAlias")
    val localKeystorePasswdAlias = config.getString("credential.localKeystorePasswdAlias")
    val jceksPath = config.getString("credential.jceksPath")

    logger.info(s"Params extracted are : \njceksPath: $jceksPath, keySecureLoginAlias: $keySecureLoginAlias keySecurePasswdAlias:$keySecurePasswdAlias, localKeystoreLoginAlias: $localKeystoreLoginAlias, localKeystorePasswdAlias: $localKeystorePasswdAlias")
    (jceksPath, keySecureLoginAlias, keySecurePasswdAlias, localKeystoreLoginAlias, localKeystorePasswdAlias)

  }


}
