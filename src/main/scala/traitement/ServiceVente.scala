package traitement

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame




object ServiceVente {

  implicit class DataFrameUtils(dataFrame: DataFrame) {

    def formatter() = {
      dataFrame.withColumn("HTT", split(col("HTT_TVA"), "\\|")(0))
        .withColumn("TVA", split(col("HTT_TVA"), "\\|")(1))
    }

    def calculTTC() : DataFrame = {

      val modifiedDF = dataFrame.withColumn("HTT", regexp_replace(col("HTT"), ",", ".").cast("double"))
        .withColumn("TVA", regexp_replace(col("TVA"), ",", ".").cast("double"))


      modifiedDF.withColumn("TTC", round(col("HTT") + col("TVA")*col("HTT"),2))
        .drop("HTT")
        .drop("TVA")
    }

    def extractDateEndContratVille(): DataFrame = {
      val formattedDF = dataFrame.withColumn(
        "Date_End_contrat",
        // Use regexp_extract to extract the date part
        date_format(
          to_timestamp(
            regexp_extract(col("MetaData"), """(?<=Date_End_contrat":")(.*?)(?=")""", 0),
            "yyyy-MM-dd HH:mm:ss"
          ),
          "yyyy-MM-dd" // Format the date as YYYY-MM-DD
        )
      )
        .drop("MetaData")
      formattedDF
    }


    def contratStatus(): DataFrame = {
      dataFrame.withColumn("Contrat_Status",
        when(col("Date_End_contrat") < current_date(), "Expired")
          .otherwise("Active")
      )
    }
  }
}
