package main

import args._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import parser.ConfigurationParser
import reader.CsvReader
import traitement.ServiceVente._

object Test{

  def main(args: Array[String]): Unit = {
    // Path to your CSV file (ensure the path is correct)
    val csvFile = "src/main/resources/DataforTest/data.csv"

//    // Create SparkSession
//    val spark = SparkSession.builder
//      .appName("SimpleApplication")
//      .config("spark.master", "local") // Configuring Spark to run locally
//      .getOrCreate()
//
//    // Read the CSV file
//    val df = spark.read
//      .format("csv")
//      .option("header", "true") // Specify that the first row contains column names
//      .option("delimiter", "#") // Specify custom delimiter
//      .load(csvFile) // Load the CSV file into a DataFrame
//
//    // Show the DataFrame
//    df.show()
//
//    // Stop SparkSession (good practice)
//    spark.stop()



    val reader = ConfigurationParser.getJsonReaderConfigurationFromJson("src/main/resources/Configuration/reader_xml.json")

    implicit val spark: SparkSession = SparkSession
      .builder
      .appName("Contract-Management")
      .config("spark.master", "local")
      .getOrCreate()


//    val clientSchema = StructType(Seq(
//      StructField("Id_Client", IntegerType, nullable = true),
//      StructField("HTT_TVA", StringType, nullable = true), // Stored as a String; you can parse it later
//      StructField("MetaData", StructType(Seq(
//        StructField("MetaTransaction", ArrayType(
//          StructType(Seq(
//            StructField("Ville", StringType, nullable = true),
//            StructField("Date_End_contrat", StringType, nullable = true), // Date parsing can happen during processing
//            StructField("TypeProd", StringType, nullable = true),
//            StructField("produit", ArrayType(StringType, containsNull = true), nullable = true)
//          )),
//          containsNull = true
//        ), nullable = true)
//      )), nullable = true)
//    ))
//
//
//    val rawDf = spark.read
//      .format("xml")
//      .option("rowTag", "Client") // Specify the correct root tag
//      .option("mode", "DROPMALFORMED")  // Skip corrupt rows
//      .schema(clientSchema)
//      .load("src/main/resources/DataforTest/data.xml")
//
//    rawDf.show()
//    rawDf.printSchema()


    val df = reader.read()


    df.show(20)


  }
}
