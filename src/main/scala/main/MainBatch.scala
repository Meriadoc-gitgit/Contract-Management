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
        ConfigurationParser.getXMLReaderConfigurationFromJson(Args.readerConfigurationFile)
      }
      case _ => throw new Exception("Invalid reader type. Supported reader format : csv, json and xml in feature")

    }
//    val df=reader.read().formatter()
//    println("***********************Resultat Question1*****************************")
//    df.show(20)
//    println("***********************Resultat Question2*****************************")
//    df.calculTTC().show(20)
//    println("***********************Resultat Question3*****************************")
//    df.calculTTC.extractDateEndContratVille.show
//    println("***********************Resultat Question4*****************************")
//    df.calculTTC.extractDateEndContratVille.contratStatus.show(20)
  }
}
