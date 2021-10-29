package com.socgen.credentials

import org.slf4j.LoggerFactory
import org.apache.hadoop.security.alias.CredentialProviderFactory
import java.io.IOException

import com.socgen.poc.Anonymise
import org.apache.hadoop.conf.Configuration

/**
 * Created by X178743 on 27/04/2020.
 * Copyright Société Générale.
 */
/**
 * Allows to parse properties stored in hadoop Credentials
 */
object ParserCredentials {

  val logger = LoggerFactory.getLogger(this.getClass.getCanonicalName)
  val sc = Anonymise.spark.sparkContext


  /**
   * @param keysecure_login_alias
   * @param keysecure_password_alias
   * @throws IOException
   */
  @throws[IOException]
  def getKeySecureCredentials(keysecure_login_alias: String, keysecure_password_alias: String, jceksPath: String): KeySecureCredentials = {
    try {
      val configuration: Configuration = setFSConfiguration(jceksPath)
      val client_id = configuration.getPassword(keysecure_login_alias)
      val client_sec = configuration.getPassword(keysecure_password_alias)
      val keySecureCredentials = KeySecureCredentials(String.valueOf(client_id),String.valueOf(client_sec))
      logger.info("Get KeySecure Credentials from Provider : keySecureLogin " + keySecureCredentials.keySecureLogin + "keySecurePassword :" + keySecureCredentials.keySecurePassword)
      keySecureCredentials
    } catch {
      case e: IOException =>
        logger.error(e.getMessage + " keysecureError")
        System.exit(1)
        KeySecureCredentials("","")
      case e: NullPointerException =>
        logger.error(e.getMessage + " NullPointer keysecure")
        System.exit(1)
        KeySecureCredentials("","")
    }
  }

  /**
   * @param local_keystore_login_alias
   * @param local_keystore_password_alias
   * @throws IOException
   */
  @throws[IOException]
  def getLocalKeystoreCredentials(local_keystore_login_alias: String, local_keystore_password_alias: String, jceksPath: String): LocalKeystoreCredentials = {
    try {
      val configuration: Configuration = setFSConfiguration(jceksPath)
      val client_id = configuration.getPassword(local_keystore_login_alias)
      val client_sec = configuration.getPassword(local_keystore_password_alias)
      val keySecureCredentials = LocalKeystoreCredentials(String.valueOf(client_id),String.valueOf(client_sec))
      logger.info("Get KeySecure Credentials from Provider : keySecureLogin " + keySecureCredentials.localKeystoreLogin + "keySecurePassword :" + keySecureCredentials.localKeystorePassword)
      keySecureCredentials
    } catch {
      case e: IOException =>
        logger.error(e.getMessage+ " localKeyStore Error")
        System.exit(1)
        LocalKeystoreCredentials("","")
      case e: NullPointerException =>
        logger.error(e.getMessage + " NullPointer localKeystore")
        System.exit(1)
        LocalKeystoreCredentials("","")
    }
  }

  private def setFSConfiguration(jceksPath: String) = {
    val configuration = sc.hadoopConfiguration
    logger.info("jceks path : "+jceksPath)
    configuration.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, jceksPath)
    configuration
  }
}
