package example.features

import io.delta.tables._
import org.apache.spark.sql.SparkSession

object version {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("version control")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val delPath = "/home/avyuthan-shah/Desktop/Data/dataFF"

    // Using DeltaTable API to get the history
    val deltaTable = DeltaTable.forPath(spark, delPath)
    val historyDF = deltaTable.history() // Optionally, you can pass an integer to limit the history entries

    historyDF.show(truncate = false)

    spark.stop()
  }
}
