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
    val reader = ConfigurationParser.getXmlReaderConfigurationFromJson("src/main/resources/Configuration/reader_xml.json")

    implicit val spark: SparkSession = SparkSession
      .builder
      .appName("Contract-Management")
      .config("spark.master", "local")
      .getOrCreate()


    val testDf = reader.read()
    testDf.formatter()
    testDf.show()

  }
}
