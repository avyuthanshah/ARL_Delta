package example.features

import example.Extra.{status,folderSize}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object upsert2 extends App{
    val spark = SparkSession.builder()
      .appName("write to delta")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.databricks.delta.schema.autoMerge.enabled", "true")//automerge schema
      .config("spark.databricks.delta.autoCompact.enabled", "true") // Enable auto-compaction globally. By default will use 128 MB as the target file size.
      .config("spark.databricks.delta.optimizeWrite.enabled", "true")
      .config("spark.databricks.delta.autoCompact.targetFileSize", "128MB") // Set target file size for auto-compaction
      .config("spark.databricks.delta.optimizeWrite.binSize", "128MB") // Set bin size for optimized writes
      .config("spark.databricks.delta.optimizeWrite.binSize", "128MB")  // Set bin size for optimized writes
      .config("spark.databricks.delta.optimizeWrite.numShuffleBlocks", "100000") // Set number of shuffle blocks
      .config("spark.databricks.delta.optimizeWrite.maxShufflePartitions", "1000")  // Set max number of output partitions

.getOrCreate()

    val delPath = "/home/avyuthan-shah/Desktop/Data/dataFFF2"


    val schema = StructType(Array(
        StructField("AccountNo", StringType, nullable = false),
        StructField("DATE", StringType, nullable = false),
        StructField("TRANSACTIONDETAILS", StringType, nullable = false),
        StructField("CHQNO", StringType, nullable = true),
        StructField("VALUEDATE", StringType, nullable = false),
        StructField("WITHDRAWALAMT", DoubleType, nullable = true),
        StructField("DEPOSITAMT", DoubleType, nullable = true),
        StructField("BALANCEAMT", DoubleType, nullable = true),
        StructField("TIME",StringType, nullable = true)
    ))

    val df:DataFrame=spark.read
      .option("header","true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "false")
      //      .option("overwriteSchema","true")
      .schema(schema)
      .csv("/home/avyuthan-shah/Desktop/F1Intern/Datasets/Bank_transaction/Fake/banktrans5M.csv")

    import spark.implicits._  //to use to_date and get map schemas

    val filtered_df=df
      .withColumn("DATE", to_date($"DATE", "yyyy-MM-dd"))
      .withColumn("VALUEDATE", to_date($"VALUEDATE", "yyyy-MM-dd"))

    filtered_df.show()

  
    val initialFsize=folderSize.getCurrentFolderSize(delPath)
    status.writeFile("true")
    val timeStart=System.nanoTime()
    filtered_df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema","true")//Schema Evolvement in New Version removing old data
      .option("optimizeWrite", "true")
      .option("autoCompact", "true")
      .partitionBy("AccountNo")//partition by to optimize query for filtering based on AccountNo or you can use by time, date or according to use
      .save(delPath)
    val timeEnd = System.nanoTime()
    status.writeFile("false")
    val elapsedTimeS = (timeEnd - timeStart) / 1e9 // Time in seconds
    val finalFsize=folderSize.getCurrentFolderSize(delPath)

    
    val startTime=System.nanoTime()

    val deltaT=DeltaTable.forPath(spark,delPath)
    deltaT.optimize().executeCompaction()

    val endTime = System.nanoTime()

    val elapsedTime = (endTime - startTime) / 1e9 // Time in seconds



    println()
    println(s"Elapsed time to for schema overwrite: $elapsedTimeS")
    println(s"Elapsed time to for compaction: $elapsedTime")
    println(s"Folder size before Schema overwrite: $initialFsize")
    println(s"Folder size after Schema Overwrite: $finalFsize")
    println()
    spark.stop()
}
