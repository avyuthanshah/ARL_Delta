package example.features

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import example.Extra.{folderSize,status}
object cleanDelta extends App{
  val spark = SparkSession.builder()
    .appName("Clean Data")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")//Only add this at your own risk to remove all the old versions and only keep latest one
    .config("spark.databricks.delta.optimize.repartition.enabled","false")
//    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
//    .config("spark.databricks.delta.autoCompact.enabled", "true") // Enable auto-compaction globally. By default will use 128 MB as the target file size.
//    .config("spark.databricks.delta.optimizeWrite.enabled", "true") // Enable optimized writes globally.
//    .config("spark.databricks.delta.autoCompact.targetFileSize", "256MB") // Set target file size for auto-compaction
//    .config("spark.databricks.delta.autoCompact.minNumFiles","10")//When the number of small files in a Delta Lake table exceeds the specified minimum (in this case, 10),Delta Lake will automatically trigger a compaction process to combine these small files into larger ones.
//    .config("spark.databricks.delta.optimizeWrite.binSize", "256MB") // Set bin size for optimized writes
    .getOrCreate()

  val delPath="/home/avyuthan-shah/Desktop/Data/dataFFF"

  val dT=spark.read.format("delta").load(delPath)
  dT.printSchema()
//  import spark.implicits._
//  val clean_dT=dT
//    .withColumn("AccountNo", regexp_replace($"AccountNo", "'", ""))
//  clean_dT.write.mode("overwrite").format("delta").save(delPath)

  val initialFsize=folderSize.getCurrentFolderSize(delPath)
  // status.writeFile("true")//to monitor load
  // val startTime = System.nanoTime()

   val deltaTable = DeltaTable.forPath(spark, delPath)
//    deltaTable.optimize().executeCompaction()
//  deltaTable.optimize().executeCompaction()
//  deltaTable.vacuum()
   deltaTable.vacuum(retentionHours=48)
  // val endTime = System.nanoTime()

  // status.writeFile("false")
  // val finalFsize=folderSize.getCurrentFolderSize(delPath)
  // val elapsedTime = (endTime - startTime) / 1e9 // Time in seconds
  // println(s"Elapsed time for vacuum: '$elapsedTime' ")
  // println(s"Initial Folder Size: '$initialFsize' ")
  // println(s"Final Folder Size '$finalFsize' ")


//  status.writeFile("true")//to monitor load
//  val startTime = System.nanoTime()
//  val dt=DeltaTable.forPath(spark, delPath)
//  dt.optimize().executeZOrderBy("AccountNo")
//  val endTime = System.nanoTime()
//  status.writeFile("false")
//  val elapsedTime = (endTime - startTime) / 1e9 // Time in seconds
//  println(s"Elapsed time for Zordering: '$elapsedTime' ")
//
  val finalFsize=folderSize.getCurrentFolderSize(delPath)
  println(s"Initial Folder Size: '$initialFsize' ")
  println(s"Final Folder Size '$finalFsize' ")
}
