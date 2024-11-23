import org.apache.spark.sql.SparkSession
object MyFirstApp{
  def main(args: Array[String]) : Unit = {
    val csvFile="src/main/resources/DataforTest/data.csv"

    val spark = SparkSession.builder.appName("SimpleApplication")
      .config("spark.master", "local")
      .getOrCreate()

    val df = spark.read
      .format("csv")
      .option("header", "true") // Si le fichier CSV contient un en-tête
      .option("delimiter","#")
      .load(csvFile)

    df.show()

  }
}