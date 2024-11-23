package parser

// import play.api.libs.json._

import scala.io.Source;
import org.apache.spark.sql.SparkSession

object ConfigurationParser {

    def main(args: Array[String]) : Unit = {

        val spark = SparkSession.builder.appName("SimpleApplication")
            .config("spark.master", "local")
            .getOrCreate()
        val df = spark.read
            .format("csv")
            .option("header", "true") // Si le fichier CSV contient un en-tête
            .load("src/main/resources/DataforTest/data.json")

        df.show()
    }

}
