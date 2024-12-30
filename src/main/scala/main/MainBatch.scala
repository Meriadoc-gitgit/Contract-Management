package main

import org.apache.spark.sql.SparkSession
import args._
import parser.ConfigurationParser
import traitement.ServiceVente._

object MainBatch {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder
      .appName("Contract-Management")
      .config("spark.master", "local")
      .getOrCreate()

    Args.parseArguments(args)
    val reader = Args.readertype match {
      case "csv" => {
        ConfigurationParser.getCsvReaderConfigurationFromJson(Args.readerConfigurationFile)
      }
      case "json" => {
        ConfigurationParser.getJsonReaderConfigurationFromJson(Args.readerConfigurationFile)
      }
      case "xml" => {
        ConfigurationParser.getXmlReaderConfigurationFromJson(Args.readerConfigurationFile)
      }
      case _ => throw new Exception("Invalid reader type. Supported reader format : csv, json and xml in feature")

    }
    val df=reader.read().formatter()
    assert(df.select("HTT_TVA").count() > 0, "DataFrame is empty")


    println("***********************Resultat Question1*****************************")
    df.show(20)
    println("***********************Resultat Question2*****************************")
    df.calculTTC().show(20)
    println(df.calculTTC.select("TTC").where(df.col("Id_Client")===1)==119.6, "Error in CalculTTC")

    println("***********************Resultat Question3*****************************")
    df.calculTTC.extractDateEndContratVille.show
    println(df.calculTTC.extractDateEndContratVille.select("Date_End_contrat").where(df.col("Id_Client")===1)=="2020-12-23", "Error in extractDateEndContratVille")

    println("***********************Resultat Question4*****************************")
    df.calculTTC.extractDateEndContratVille.contratStatus.show(20)
    println(df.calculTTC.extractDateEndContratVille.contratStatus.select("Contrat_Status").where(df.col("Id_Client")===1)=="Expired", "Error in contratStatus")




    // Exec test
//    implicit val spark: SparkSession = SparkSession
//      .builder
//      .appName("Contract-Management")
//      .config("spark.master", "local")
//      .getOrCreate()
//
//
//    // CSV
//    println("\n\nCsvReader\n\n")
//    val reader1 = ConfigurationParser.getCsvReaderConfigurationFromJson("src/main/resources/Configuration/reader_csv.json")
//
//    val testDf1 = reader1.read()
//    //    testDf1.formatter().show()
//    //    testDf1.show()
//    testDf1.formatter.calculTTC.extractDateEndContratVille.contratStatus.show()
//
//    // JSON
//    println("\n\nJsonReader\n\n")
//    val reader2 = ConfigurationParser.getJsonReaderConfigurationFromJson("src/main/resources/Configuration/reader_json.json")
//
//    val testDf2 = reader2.read()
//    //    testDf2.formatter().show()
//    //    testDf2.show()
//    testDf2.formatter.calculTTC.extractDateEndContratVille.contratStatus.show()
//
//    // XML
//    println("\n\nXmlReader\n\n")
//    val reader3 = ConfigurationParser.getXmlReaderConfigurationFromJson("src/main/resources/Configuration/reader_xml.json")
//
//    val testDf3 = reader3.read()
//    //    testDf3.formatter().show()
//    //    testDf3.show()
//    testDf3.formatter.calculTTC.extractDateEndContratVille.contratStatus.show()
  }
}
