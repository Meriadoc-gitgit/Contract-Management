package main

import args._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.json4s.jackson.JsonMethods
import parser.{ConfigurationParser, FileReaderUsingIOSource}
import reader._
import traitement.ServiceVente._
import scala.xml._
import scala.io.Source


object Test {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder
      .appName("Contract-Management")
      .config("spark.master", "local")
      .getOrCreate()


    // CSV
    println("\n\nCsvReader\n\n")
    val reader1 = ConfigurationParser.getCsvReaderConfigurationFromJson("src/main/resources/Configuration/reader_csv.json")

    val testDf1 = reader1.read()
    testDf1.formatter().show()
//    testDf1.show()

    // JSON
    println("\n\nJsonReader\n\n")
    val reader2 = ConfigurationParser.getJsonReaderConfigurationFromJson("src/main/resources/Configuration/reader_json.json")

    val testDf2 = reader2.read()
    testDf2.formatter().show()
//    testDf2.show()

    // XML
    println("\n\nXmlReader\n\n")
    val reader3 = ConfigurationParser.getXmlReaderConfigurationFromJson("src/main/resources/Configuration/reader_xml.json")

    val testDf3 = reader3.read()
    testDf3.formatter().show()
//    testDf3.show()

  }
}
