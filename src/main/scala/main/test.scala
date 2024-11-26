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




    println(FileReaderUsingIOSource.getContent("src/main/resources/Configuration/reader_xml.json"))
    println(JsonMethods.parse(
      FileReaderUsingIOSource.getContent("src/main/resources/Configuration/reader_xml.json")
    ) \ "path")
    val reader = ConfigurationParser.getXmlReaderConfigurationFromJson("src/main/resources/Configuration/reader_xml.json")

//    println(reader.trimEdges("<MetaTransaction>"))

//    implicit val spark: SparkSession = SparkSession
//      .builder
//      .appName("Contract-Management")
//      .config("spark.master", "local")
//      .getOrCreate()
//
//    reader.test()
//    df.show()



//    val xmlString: String = Source.fromFile("src/main/resources/DataforTest/data.xml", "UTF-8").getLines().mkString("\n")
    val filePath = "src/main/resources/DataforTest/data.xml" // Path to your XML file

    // Read the entire content of the XML file into a string
    val xmlString: String = Source.fromFile(filePath).mkString


    // Parse the XML string into a Map
    val parsedMap = reader.parseXMLDocument(xmlString)

    println("Parsed Map: \n"+parsedMap)


    // Print the parsed Map
//    println(parsedMap)

    val cleanM = reader.postProcess(reader.cleanMap(parsedMap))
//    println(cleanM+"\n")

    var m = cleanM.asInstanceOf[List[_]]


    var h = m.head.asInstanceOf[Map[String, Any]]


    var li = List[Map[String, Any]]()  // Use var to allow reassignment

    m.foreach { element =>
      val h = element.asInstanceOf[Map[String, Any]]

      // Ensure h.values.head is a Map[String, Any]
      h.values.head match {
        case map: Map[String, Any] =>
          li = map :: li  // Prepend the map to the list
        case _ =>
          println("Value is not a Map[String, Any]")
      }
    }



    val extraM = reader.extra(cleanM).asInstanceOf[List[Map[String, Any]]]

    val tag = reader.combineTag(extraM)
    println("\n===============\n"+tag+"\n===============\n")

    val header = tag.head.asInstanceOf[Map[String, Any]].keys


    val md = tag.head.asInstanceOf[Map[String, Any]]("MetaData").asInstanceOf[List[_]].head

    val jsonTest = reader.transformToJson(md.asInstanceOf[Map[String, List[Map[String, List[String]]]]])


    println(jsonTest)

    val map = tag.head.asInstanceOf[Map[String, Any]]

    // Remove the "MetaData" key
    var updatedMap = map - "MetaData"

    println("here"+updatedMap) // Map(OtherKey -> value2)

    updatedMap = updatedMap + ("MetaData" -> jsonTest)

    println("here2"+updatedMap) // Map(OtherKey -> value2)



    // ----------------------------------

    val md2 = tag.head.asInstanceOf[Map[String, Any]]("Id_Client").asInstanceOf[List[_]].head

    println("\n"+md2+"\n")

//    val jsonM = reader.mapToJson(MainList)

    var lr = List[Any]()
    tag.asInstanceOf[List[Map[String, List[_]]]].foreach { m =>
      var mapTmp = Map[Any, Any]()
      m.foreach { m1 =>
        if (m1._2.head.isInstanceOf[String]) {
  //        println("ok"+m1._2.head)
          mapTmp = mapTmp + (m1._1 -> s"${m1._2.head.toString}")
        }
        else {
          val jT = reader.transformToJson(m1._2.head.asInstanceOf[Map[String, List[Map[String, List[String]]]]])
//          println(jT+"\n")
          mapTmp = mapTmp + (m1._1 -> jT)
        }
      }
      lr = mapTmp :: lr
    }




    println("\nHERE\n"+reader.transformToJsonList(reader.defineListMapString(tag.asInstanceOf[List[Map[String, List[_]]]]))+"\n\n")


    reader.writeJsonToFile(reader.transformToJsonList(reader.defineListMapString(tag.asInstanceOf[List[Map[String, List[_]]]])), "src/main/resources/DataforTest/test.json")



    implicit val spark: SparkSession = SparkSession
      .builder
      .appName("Contract-Management")
      .config("spark.master", "local")
      .getOrCreate()


    val df = spark.read.format("json")
      .option("multiline", true)
      .load("src/main/resources/DataforTest/test.json")

    df.formatter()
//    df.show()



    val testDf = reader.read()
    testDf.formatter()
    testDf.show()




//    val filePath2 = "src/main/resources/DataforTest/test.json"  // Specify the path to your output file
////    reader.writeJsonToFile(jsonM, filePath2)
//
//    implicit val spark: SparkSession = SparkSession
//      .builder
//      .appName("Contract-Management")
//      .config("spark.master", "local")
//      .getOrCreate()
//
//    val df = spark.read.format("json")
//      .option("multiline","true")
//      .load("src/main/resources/DataforTest/test.json")
//
//    df.formatter()
//    df.show()









    //    val df = spark.read.format("json")
//      .load(filePath2)




//    println(df.columns(0))
//    df.select(df.columns(0)).show()
//    df.printSchema()




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



  }
}
