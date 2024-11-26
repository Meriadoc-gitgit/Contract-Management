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

    def getJsonReaderConfigurationFromJson(jsonString: String): JsonReader = {
        JsonMethods.parse(
            FileReaderUsingIOSource.getContent(jsonString)
        ).extract[JsonReader]
    }

    def getXmlReaderConfigurationFromJson(jsonString: String): XmlReader = {
        JsonMethods.parse(
            FileReaderUsingIOSource.getContent(jsonString)
        ).extract[XmlReader]
    }
}
