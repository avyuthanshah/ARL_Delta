package example

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import example.Extra.status

object exc2delt {
  def main(args: Array[String]): Unit = {
    println("Excel To Delta Table")
    val spark=SparkSession.builder()
      .appName("ExcelToDelta")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    val schema = StructType(Array(
      StructField("AccountNo", StringType, nullable = true),
      StructField("DATE", DateType, nullable = false),
      StructField("TRANSACTIONDETAILS", StringType, nullable = true),
      StructField("CHQNO", StringType, nullable = true),
      StructField("VALUEDATE", DateType, nullable = false),
      StructField("WITHDRAWALAMT", DoubleType, nullable = true),
      StructField("DEPOSITAMT", DoubleType, nullable = true),
      StructField("BALANCEAMT", DoubleType, nullable = true)
    ))


    val df:DataFrame=spark.read
      .format("com.crealytics.spark.excel")
      .option("header","true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "false")
      .schema(schema)
      .load("/home/avyuthan-shah/Desktop/F1Intern/Datasets/Bank_transaction/bank.xlsx")

    df.show()

    val repartitioned_df = df.repartition(4)
    //df.write.mode("overwrite").format("parquet").save("/home/avyuthan-shah/Desktop/F1Intern/Datasets/deltaBank_transaction/deltaBT")
    status.writeFile("true")
    Thread.sleep(100)

    val startTime=System.nanoTime()
    repartitioned_df.write
      .mode("overwrite")
      .format("delta")
      .save("/home/avyuthan-shah/Desktop/Data/dataF_excel")
    val endTime = System.nanoTime()

    status.writeFile("false")

    val elapsedTime = (endTime - startTime) / 1e9 // Time in seconds
    println()
    println(s"Elapsed Time to upload into delta from excel : $elapsedTime")
    println()
    spark.stop()
  }
}