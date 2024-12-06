package traitement

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._


object ServiceVente {

  implicit class DataFrameUtils(dataFrame: DataFrame) {

    def formatter() = {
      dataFrame.withColumn("HTT", split(col("HTT_TVA"), "\\|")(0))
        .withColumn("TVA", split(col("HTT_TVA"), "\\|")(1))
    }

//    def calculTTC () : DataFrame = {
//      /*..........................coder ici...............................*/
//    }
//    def extractDateEndContratVille(): DataFrame = {
//      val schema_MetaTransaction = new StructType()
//        .add("Ville", StringType, false)
//        .add("Date_End_contrat", StringType, false)
//      val schema = new StructType()
//        .add("MetaTransaction", ArrayType(schema_MetaTransaction), true)
//      /*..........................coder ici...............................*/
//
//    }
//
//    def contratStatus(): DataFrame = {
//      /*..........................coder ici...............................*/
//    }


  }

}
from pyspark.sql.functions import regexp_extract

df_with_columns = df.select(
    # Extraire la date à l'aide d'une expression régulière
    regexp_extract(col("json_column"), r'"Date_End_contrat":\s?'data.json', 1).alias("Date_End_contrat"),
    # Extraire la ville à l'aide d'une expression régulière
    regexp_extract(col("json_column"), r'"Ville":\s?'data.json', 1).alias("Ville")
)

df_with_columns.show()
