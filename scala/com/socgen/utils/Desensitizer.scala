package com.socgen.utils


import java.sql.Date
import java.util.logging.Logger

import com.socgen.credentials.ParserCredentials
import com.socgen.poc.Anonymise
import org.apache.spark.sql.functions._
import com.socgen.sdt.anonymization.hash.ColumnHasher
import com.socgen.sdt.anonymization.model.{HashAlgorithm, Sha1, Sha256}
import com.socgen.sdt.anonymization.substitution.{ColumnConsistentRandomizer, ColumnRandomizer}
import com.socgen.sdt.anonymization.uniformization.{ColumnBlanker, ColumnNullifier}
import com.socgen.sdt.model.{Alphabet, DigitAlphabet, LatinAnyCaseWithAccentsAndPunctuation}
import com.socgen.sdt.pseudonymization.encryption.{ColumnConsistentAndFormatPreservingEncryptor, ConsistentAndFormatPreservingEncryptorDecryptor, KmipProperties}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, StructType}


/**
 * Created by X178743 on 14/06/2019.
 * Copyright Société Générale.
 *
 * This Object id to implement Desensitization Methods
 *
 */

object Desensitizer {

  /**
   * in this section we initialize casa specific properties to use cryptography capacities
   * key name depending on specific environment
   * key version
   * default initialization vector used for calculating the value of the next encryption
   * kmip properties to init and to connect to the keystore
   * This part should be externalized and secured using hadoop credential
   *
   */

  val (keyName, keyVersion, keySecureServerURL, keySecureServerPort, localKeystorePath, keysecureUsername, keysecureUserPassword, localKeystoreAlias,localKeystorePassword) = TableParamsExtractor.getKmipPropertiesParams()
  val (jceksPath, keysecureLoginAlias, keysecurePAsswdAlias, localKeystoreLoginAlias, localKeystorePasswdAlias) = TableParamsExtractor.getCredentialPropertiesParams()
  val keysecureCred = ParserCredentials.getKeySecureCredentials(keysecureLoginAlias, keysecurePAsswdAlias, jceksPath)
  val localKeystoreCred = ParserCredentials.getLocalKeystoreCredentials(localKeystoreLoginAlias, localKeystorePasswdAlias, jceksPath)


  val DEFAULT_INITIALIZATION_VECTOR: Array[Byte] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-|% ".getBytes
  val FPE_KEY_NAME = keyName
  val FPE_KEY_VERSION = keyVersion
  val kmipPropertiesForFPE: KmipProperties = {
    KmipProperties(
      keysecureServerUrl = keySecureServerURL,
      keysecureServerPortDev = keySecureServerPort,
      keysecureUsername = keysecureCred.keySecureLogin,
      keysecureUserPassword = keysecureCred.keySecurePassword,
      localKeystorePath = localKeystorePath,
      localKeystoreAlias = localKeystoreCred.localKeystoreLogin,
      localKeystorePassword = localKeystoreCred.localKeystorePassword)
  }


  def consistentAndFormatPreservingEncryptorDecryptor = ConsistentAndFormatPreservingEncryptorDecryptor(FPE_KEY_NAME, FPE_KEY_VERSION, kmipPropertiesForFPE, DEFAULT_INITIALIZATION_VECTOR)


  // Declare variable that holds desensitization options provided by the user
  val logger = Logger.getLogger(this.getClass.getName)
  var RESPECT_TYPE = false
  var DATA_TYPE = "String"
  var KEEP_DOMAIN = false
  var KEEP_INDICATOR = true
  var CELL_PHONE = false
  var CHAR = '*'
  var ZEROREPLACECHAR = '0'
  var REMOVE_BEFORE_BLANK  = List("")
  var LENGTH = -1
  var PRESERVE_SPACES = false
  var INT_BOUNDS : Option[(Int,Int)] = None
  var LONG_BOUNDS : Option[(Long,Long)] = None
  var FLOAT_BOUNDS : Option[(Float,Float)] = None
  var DOUBLE_BOUNDS : Option[(Double,Double)] = None
  var DATE_BOUNDS = None
  var SALT : Option[String] = None


  /**
   * Gets Hive Trigger Table as A DataFrame by aggregating configuration DF and dataBase and table names.
   *
   * @param desensitizationType desensitization method provided by the user.
   * @param crossOptions   Desensitization method options provided by the user.
   * @return anonymous function that will call the appropriate data anonymizer method
   */
  def desensitizeDataFrame( desensitizationType: String, crossOptions : Option[List[(String, String)]]): (DataFrame, String) => DataFrame = {
    crossOptions match {
      case Some(x) => x.foreach(setOptionValuePair)
      case None => println("No Option Provided ! Continuing Desensitization")
    }

    val desensitizationFunction = getDesensitizationFunction(desensitizationType)
    desensitizationFunction
  }

  /**
   * Gets Hive Trigger Table as A DataFrame by aggregating configuration DF and dataBase and table names.
   *
   * @param optionValuePair desensitization method provided by the user.
   * @return anonymous function that will call the appropriate data anonymizer method
   */
  def setOptionValuePair(optionValuePair : (String, String)) = {
    optionValuePair match {
        case ("respect_type", "true") => RESPECT_TYPE = true
        case ("data_type",_) => DATA_TYPE = optionValuePair._2.trim
        case ("keep_domaine","true") => KEEP_DOMAIN = true
        case ("keep_indicator","false") => KEEP_INDICATOR = false
        case ("cell_phone","true") => CELL_PHONE = true
        case ("remove_before_blank",_) => REMOVE_BEFORE_BLANK = optionValuePair._2.trim.split(";").toList
        case ("length", _) => LENGTH = Integer.parseInt(optionValuePair._2.trim)
        case ("char",_) => CHAR = optionValuePair._2.trim.charAt(0)
        case ("preserve_spaces","true") => PRESERVE_SPACES = true
        case ("int_bounds", _) => setIntBoundsFromStr(optionValuePair._2.trim)
        case ("long_bounds", _) => setLongBoundsFromStr(optionValuePair._2.trim)
        case ("float_bounds", _) => setFloatBoundsFromStr(optionValuePair._2.trim)
        case ("double_bounds", _) => setDoubleBoundsFromStr(optionValuePair._2.trim)
        case ("date_bounds", _) => println("date bounds")
        case ("salt", _) => Option(optionValuePair._2.trim)
        case _ => println("No Option Provided Or Misprovided ! Continuing Desensitization")
    }
  }

  /**
   * Gets Hive Trigger Table as A DataFrame by aggregating configuration DF and dataBase and table names.
   *
   * @param desensitizationType desensitization method provided by the user.
   * @return anonymous function that will call the appropriate data anonymizer method
   */
  def getDesensitizationFunction(desensitizationType: String): (DataFrame, String) => DataFrame = {
    desensitizationType match {
      case "id" => ColumnConsistentRandomizer.encryptAlphanumericChars
      case "chaine" => ColumnConsistentRandomizer.encryptAlphanumericChars
      case "numeric" => ColumnConsistentRandomizer.encryptNumericalValues
      case "iban" => ColumnConsistentRandomizer.encryptIban
      case "firstname" => ColumnRandomizer.randomizeWithFirstNames
      case "lastname" => ColumnRandomizer.randomizeWithLastnames
      case "card" => ColumnBlanker.blankCreditCard
      case "email" => randomizeEmail(_, _, KEEP_DOMAIN)
      case "tel" => randomizePhoneNumber(_, _, KEEP_INDICATOR, CELL_PHONE)
      case "blankedWithChar" => ColumnBlanker.blankWithChars(_, _, CHAR, REMOVE_BEFORE_BLANK : _*)
      case "XReplace" => ColumnBlanker.blankWithChars(_,_, CHAR, List(" ", "-", "/") : _*)
      case "0Replace" => ColumnBlanker.blankWithChars(_,_, ZEROREPLACECHAR, List(" ", "-", "/") : _*)
      case "blank" => blank(_, _, RESPECT_TYPE, DATA_TYPE)
      case "null" => nullify(_, _, RESPECT_TYPE, DATA_TYPE)
      case "randomAlphabeticChars" => randomizeAlphabeticChars(_, _, LENGTH, PRESERVE_SPACES)
      case "randomNumericChars" => randomizeNumericChars(_, _, LENGTH, PRESERVE_SPACES)
      case "randomInts" => randomizeInt(_, _, None)
      case "randomLongs" => randomizeLong(_, _, None)
      case "randomFloats" => randomizeFloat(_, _, None)
      case "randomDoubles" => randomizeDouble(_, _, None)
      case "randomDates" => randomizeDate(_, _, None)
      case "randomBooleans" => ColumnRandomizer.randomizeWithBooleans
      case "keepDistribution" => randomizeKeepingDistribution
      case "hashWithSha1" => hash(_, _, Sha1, None)
      case "hashWithSha256" => hash(_, _, Sha256, None)
      case "consistentNumericFormatPreserving" => ColumnConsistentAndFormatPreservingEncryptor.encryptIntWithConsistencyAndFormatPreservation(_, _,consistentAndFormatPreservingEncryptorDecryptor)
      case "consistentStringFormatPreserving" => ColumnConsistentAndFormatPreservingEncryptor.encryptString(_, _,consistentAndFormatPreservingEncryptorDecryptor, Alphabet(LatinAnyCaseWithAccentsAndPunctuation.chars ++ DigitAlphabet.chars ++ Alphabet("%=+/`").chars))
      case "consistentNumericFormatPreservingDecryption" => ColumnConsistentAndFormatPreservingEncryptor.decryptIntWithConsistencyAndFormatPreservation(_, _,consistentAndFormatPreservingEncryptorDecryptor)
      case "consistentStringFormatPreservingDecryption" => ColumnConsistentAndFormatPreservingEncryptor.decryptString(_, _,consistentAndFormatPreservingEncryptorDecryptor, Alphabet(LatinAnyCaseWithAccentsAndPunctuation.chars ++ DigitAlphabet.chars ++ Alphabet("%=+/`").chars))
    }
  }


  def setIntBoundsFromStr(optionValue : String) = {
    val valuesStr = optionValue.substring(1,optionValue.size - 1)
    val splitValues = valuesStr.split(";")
    val minBound = Integer.parseInt(splitValues(0))
    val maxBound = Integer.parseInt(splitValues(1))
    INT_BOUNDS = Option((minBound,maxBound))
  }

  def setLongBoundsFromStr(optionValue : String) = {
    val valuesStr = optionValue.substring(1,optionValue.size - 1)
    val splitValues = valuesStr.split(";")
    val minBound = Integer.parseInt(splitValues(0))
    val maxBound = Integer.parseInt(splitValues(1))
    LONG_BOUNDS = Option((minBound,maxBound))
  }

  def setFloatBoundsFromStr(optionValue : String) = {
    val valuesStr = optionValue.substring(1,optionValue.size - 1)
    val splitValues = valuesStr.split(";")
    val minBound = Integer.parseInt(splitValues(0))
    val maxBound = Integer.parseInt(splitValues(1))
    FLOAT_BOUNDS = Option((minBound,maxBound))
  }

  def setDoubleBoundsFromStr(optionValue : String) = {
    val valuesStr = optionValue.substring(1,optionValue.size - 1)
    val splitValues = valuesStr.split(";")
    val minBound = Integer.parseInt(splitValues(0))
    val maxBound = Integer.parseInt(splitValues(1))
    DOUBLE_BOUNDS = Option((minBound,maxBound))
  }

  def blank(dataFrame: DataFrame, columnToBlank: String, respectType: Boolean, dataType: String): DataFrame = {
    val maybeDataFrame = {
      if (respectType) ColumnBlanker.blank(dataFrame, columnToBlank)
      else ColumnBlanker.blank(dataFrame, columnToBlank, convertStringToDataType(dataType))
    }
    maybeDataFrame.get
  }

  def nullify(dataFrame: DataFrame, columnToNullify: String, respectType: Boolean, dataType: String): DataFrame = {
    if (respectType) ColumnNullifier.nullify(dataFrame, columnToNullify)
    else ColumnNullifier.nullify(dataFrame, columnToNullify, convertStringToDataType(dataType))
  }

  def convertStringToDataType(string: String): DataType = {
    val schema = new StructType().add("column", string)
    schema.fields.head.dataType
  }

  def randomizePhoneNumber(dataFrame: DataFrame, columnToRandomize: String, keepIndicator: Boolean, cellPhone: Boolean): DataFrame = {
    if (keepIndicator) ColumnRandomizer.randomizePhoneNumberKeepingRegionIndicator(dataFrame, columnToRandomize)
    else ColumnRandomizer.randomizeWithPhoneNumbers(dataFrame, columnToRandomize, cellPhone)
  }

  def randomizeEmail(dataFrame: DataFrame, columnToRandomize: String, keepDomain: Boolean): DataFrame = {
    if (keepDomain) ColumnRandomizer.randomizeEmailPreservingLengthAndDomain(dataFrame, columnToRandomize)
    else ColumnRandomizer.randomizeWithEmailAddresses(dataFrame, columnToRandomize)
  }

  def randomizeAlphabeticChars(dataFrame: DataFrame, columnToRandomize: String, length: Int, preserveSpaces: Boolean): DataFrame = {
    if (length > 0) ColumnRandomizer.randomizeWithAlphabeticStrings(dataFrame, columnToRandomize, length)
    else if (preserveSpaces) ColumnRandomizer.randomizeWithAlphabeticStringsPreservingLengthAndSpaces(dataFrame, columnToRandomize)
    else ColumnRandomizer.randomizeWithAlphabeticStringsPreservingLength(dataFrame, columnToRandomize)
  }

  def randomizeNumericChars(dataFrame: DataFrame, columnToRandomize: String, length: Int, preserveSpaces: Boolean): DataFrame = {
    if (length > 0) ColumnRandomizer.randomizeWithNumericStrings(dataFrame, columnToRandomize, length)
    else if (preserveSpaces) ColumnRandomizer.randomizeWithNumericStringsPreservingLengthAndSpaces(dataFrame, columnToRandomize)
    else ColumnRandomizer.randomizeWithNumericStringsPreservingLength(dataFrame, columnToRandomize)
  }

  def randomizeInt(dataFrame: DataFrame, columnToRandomize: String, bounds: Option[(Int, Int)]): DataFrame = {
    bounds match {
      case Some((lowerBound, upperBound)) => ColumnRandomizer.randomizeWithInts(dataFrame, columnToRandomize, lowerBound, upperBound)
      case None => ColumnRandomizer.randomizeWithInts(dataFrame, columnToRandomize)
    }
  }

  def randomizeLong(dataFrame: DataFrame, columnToRandomize: String, bounds: Option[(Long, Long)]): DataFrame = {
    bounds match {
      case Some((lowerBound, upperBound)) => ColumnRandomizer.randomizeWithLongs(dataFrame, columnToRandomize, lowerBound, upperBound)
      case None => ColumnRandomizer.randomizeWithLongs(dataFrame, columnToRandomize)
    }
  }

  def randomizeFloat(dataFrame: DataFrame, columnToRandomize: String, bounds: Option[(Float, Float)]): DataFrame = {
    bounds match {
      case Some((lowerBound, upperBound)) => ColumnRandomizer.randomizeWithFloats(dataFrame, columnToRandomize, lowerBound, upperBound)
      case None => ColumnRandomizer.randomizeWithFloats(dataFrame, columnToRandomize)
    }
  }

  def randomizeDouble(dataFrame: DataFrame, columnToRandomize: String, bounds: Option[(Double, Double)]): DataFrame = {
    bounds match {
      case Some((lowerBound, upperBound)) => ColumnRandomizer.randomizeWithDoubles(dataFrame, columnToRandomize, lowerBound, upperBound)
      case None => ColumnRandomizer.randomizeWithDoubles(dataFrame, columnToRandomize)
    }
  }

  def randomizeDate(dataFrame: DataFrame, columnToRandomize: String, bounds: Option[(String, String)]): DataFrame = {
    bounds match {
      case Some((lowerBound, upperBound)) => ColumnRandomizer.randomizeWithDates(dataFrame, columnToRandomize, Date.valueOf(lowerBound), Date.valueOf(upperBound))
      case None => ColumnRandomizer.randomizeWithDates(dataFrame, columnToRandomize)
    }
  }

  def randomizeKeepingDistribution(dataFrame: DataFrame, columnToRandomize: String): DataFrame = {
    ColumnRandomizer.randomizeKeepingDistribution(dataFrame, columnToRandomize)
  }

  def hash(dataFrame: DataFrame, columnToHash: String, algorithm: HashAlgorithm, salt: Option[String]): DataFrame = {
    val randomSalt = "random"

    val maybeDataFrame = (algorithm, salt) match {
      case (Sha1, None) => ColumnHasher.hashWithSha1(dataFrame, columnToHash)
      case (Sha1, Some(`randomSalt`)) => ColumnHasher.hashWithSha1AndSalt(dataFrame, columnToHash)
      case (Sha1, Some(chosenSalt)) => ColumnHasher.hashWithSha1AndSalt(dataFrame, columnToHash, chosenSalt)
      case (Sha256, None) => ColumnHasher.hashWithSha2(dataFrame, columnToHash)
      case (Sha256, Some(`randomSalt`)) => ColumnHasher.hashWithSha2AndSalt(dataFrame, columnToHash)
      case (Sha256, Some(chosenSalt)) => ColumnHasher.hashWithSha2AndSalt(dataFrame, columnToHash, chosenSalt)
    }

    maybeDataFrame.get
  }

}
