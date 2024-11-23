package parser

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import reader._

object ConfigurationParser {

    implicit val format = DefaultFormats

    // Step 1: Parse the JSON string into an AST (Abstract Syntax Tree)
    // Step 2: Extract the CSV configuration from the AST and map it to the CsvReader case class

    def getCsvReaderConfigurationFromJson(jsonString: String): CsvReader = {
        JsonMethods.parse(
            FileReaderUsingIOSource.getContent(jsonString)
          ).extract[CsvReader]
    }

    def getJsonReaderConfigurationFromJson(jsonString: String): JSONReader = {
        JsonMethods.parse(
            FileReaderUsingIOSource.getContent(jsonString)
        ).extract[JSONReader]
    }

    def getXMLReaderConfigurationFromJson(jsonString: String): XMLReader = {
        JsonMethods.parse(
            FileReaderUsingIOSource.getContent(jsonString)
        ).extract[XMLReader]
    }
}
