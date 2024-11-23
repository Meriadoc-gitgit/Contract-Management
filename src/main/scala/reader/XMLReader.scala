package reader

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


case class XMLReader(
                    path: String,
                    rowTag: Option[String] = None
                    ) extends Reader {
  val format = "xml"

  def read()(implicit spark: SparkSession): DataFrame = {
    // Define new structure
    // Define the schema, with MetaData as a nested structure
    val clientSchema = StructType(Seq(
      StructField("Id_Client", IntegerType, nullable = true),
      StructField("HTT_TVA", StringType, nullable = true), // Stored as a String; you can parse it later
      StructField("MetaData", StructType(Seq(
        StructField("MetaTransaction", ArrayType(
          StructType(Seq(
            StructField("Ville", StringType, nullable = true),
            StructField("Date_End_contrat", StringType, nullable = true), // Date parsing can happen during processing
            StructField("TypeProd", StringType, nullable = true),
            StructField("produit", ArrayType(StringType, containsNull = true), nullable = true)
          )),
          containsNull = true
        ), nullable = true)
      )), nullable = true)
    ))

    spark.read.format(format)
      .option("rowTag", rowTag.getOrElse("Client"))
      .option("mode", "DROPMALFORMED") // This will skip corrupt rows
      .schema(clientSchema)
      .load(path)
  }
}
