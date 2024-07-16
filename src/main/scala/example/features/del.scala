package example.features

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object del extends App{
  val spark=SparkSession.builder()
    .appName("Fetch Delta Table")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  val delPath="/home/avyuthan-shah/Desktop/dataF"
  val deltaT=DeltaTable.forPath(spark,delPath)
  try {
    //deltaT.delete(col("AccountNo").isin("409000611074' ", "447777") && col("DATE").isNull)
    deltaT.delete(col("AccountNo").isin("557777","667777") && col("DATE").isNotNull)
    println()
    println("Deletion Completed")
    println()
  }catch {
    case e:Exception=>
      println(s"Error During deletion: ${e.getMessage}")
  }
  //val version=DeltaTable.forPath(spark, delPath).history(1).select("version").as[Long](Encoders.scalaLong).head
  //spark.read.format("delta").option("header","true").option("versionAsOf",version).load(delPath).createOrReplaceTempView("delta_table")
  spark.read.format("delta").option("header","true").load(delPath).createOrReplaceTempView("delta_table")

  // Run SQL queries on the Delta table
  val result = spark.sql(
    s"""
       |SELECT * FROM delta_table
       |WHERE AccountNo IN ("557777","667777") AND VALUEDATE IS NOT NULL;
      """.stripMargin)

  result.show(truncate=false)
}
