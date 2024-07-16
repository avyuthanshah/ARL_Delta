package example

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import io.delta.tables.DeltaTable

import example.Extra.status

object csv2del {
  def main(args: Array[String]): Unit = {
    println("CSV To Delta Table")
    val spark=SparkSession.builder()
      .appName("Csv to Delta")
      .master("local[*]")
      // .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.databricks.delta.autoCompact.enabled", "true") // Enable auto-compaction globally. By default will use 128 MB as the target file size.
      .config("spark.databricks.delta.optimizeWrite.enabled", "true")
      .config("spark.databricks.delta.autoCompact.targetFileSize", "128MB") // Set target file size for auto-compaction
      .config("spark.databricks.delta.optimizeWrite.binSize", "128MB") // Set bin size for optimized writes
      .getOrCreate()

    //Schema for bank_transaction 116k
    // val schema = StructType(Array(
    //   StructField("AccountNo", StringType, nullable = false),
    //   StructField("DATE", DateType, nullable = false),
    //   StructField("TRANSACTIONDETAILS", StringType, nullable = true),
    //   StructField("CHQNO", StringType, nullable = true),
    //   StructField("VALUEDATE", DateType, nullable = false),
    //   StructField("WITHDRAWALAMT", DoubleType, nullable = true),
    //   StructField("DEPOSITAMT", DoubleType, nullable = true),
    //   StructField("BALANCEAMT", DoubleType, nullable = true),
    //   StructField("TIME", StringType, nullable = false)
    // ))

    //schema for esewa data
    //  val schema = StructType(Array(
    //   StructField("TransactionId", StringType, nullable = false),
    //   StructField("AccountNo", StringType, nullable = false),
    //   StructField("Date", DateType, nullable = false),
    //   StructField("Time", StringType, nullable = false),
    //   StructField("Type", StringType, nullable = false),//to show wether it is withdraw or deposit
    //   StructField("Amount", DoubleType, nullable = true),
    //   StructField("BALANCEAMT", DoubleType, nullable = true),
    //   StructField("Status", StringType, nullable = false),
    //   StructField("Description", StringType, nullable = false),
    //   StructField("Channel", StringType, nullable = false)//from where transaction is initiated
    // ))

    //schema for sms data
      val schema = StructType(Array(
        StructField ("Address", StringType, nullable = false),//Address,AccountNo,Type,Amount,Date,Time,Remarks Schema for this
        StructField ("AccountNo", StringType, nullable = false),
        StructField ("Type", StringType, nullable = false),
        StructField ("Amount", DoubleType, nullable = true),
        StructField ("Date", DateType, nullable = false),
        StructField ("Time", StringType, nullable = false),
        StructField ("Remarks", StringType, nullable = false)
      ))

    //Schema For rw_transaction
//    val schema = StructType(Array(
//      StructField("txn_id", LongType, nullable = false),
//      StructField("last_modified_date", StringType, nullable = false),
//      StructField("last_modified_date_bs", StringType, nullable = false),
//      StructField("created_date", StringType, nullable = false),
//      StructField("amount", DoubleType, nullable = false),
//      StructField("status", IntegerType, nullable = false),
//      StructField("module_id", IntegerType, nullable = false),
//      StructField("product_id", IntegerType, nullable = false),
//      StructField("product_type_id", IntegerType, nullable = false),
//      StructField("payer_account_id", IntegerType, nullable = false),
//      StructField("receiver_account_id", IntegerType, nullable = false),
//      StructField("reward_point", IntegerType, nullable = false),
//      StructField("cash_back_amount", DoubleType, nullable = false),
//      StructField("revenue_amount", DoubleType, nullable = false),
//      StructField("transactor_module_id", IntegerType, nullable = false),
//      StructField("time", StringType, nullable = false) Address,AccountNo,Type,Amount,Date,Time,Remarks
//    ))

    val df:DataFrame=spark.read
      .option("header","true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "false")
      .schema(schema)
      .csv("/home/avyuthan-shah/Desktop/F1Intern/Datasets/sms.csv")
      // .csv("/home/avyuthan-shah/Desktop/F1Intern/Datasets/Bank_transaction/Fake/banktrans1k.csv")
//      .csv("/home/avyuthan-shah/Desktop/F1Intern/Datasets/Bank_transaction/bank.csv")
    
    df.show()
    df.printSchema()

    // import spark.implicits._
    //Filtering Csv file to remove ambiguity during read
//    val filtered_df = df
//      .withColumn("last_modified_date", to_date($"last_modified_date", "yyyy-MM-dd"))
//      .withColumn("created_date", to_date($"created_date", "yyyy-MM-dd"))
//      .withColumn("time", date_format(to_timestamp($"time", "HH:mm:ss"), "HH:mm:ss"))

    // val filtered_df = df
//      .withColumn("AccountNo", regexp_replace($"AccountNo", "'", ""))
      // .withColumn("DATE", to_date($"DATE", "dd-MM-yy"))
      // .withColumn("VALUEDATE", to_date($"VALUEDATE", "dd-MM-yy"))

    // filtered_df.show()
    // val repartitioned_df = filtered_df.repartition(1)//,col("AccountNo"))

    val delPath="/home/avyuthan-shah/Desktop/Data/dataFesewa"
    //path for hadoop
    val delPathHadoop="hdfs://localhost:9000/delta/dataFsms"

    // status.writeFile("true")
    // Thread.sleep(100)//delay for monitoring

//    repartitioned_df.write
//      .mode("overwrite")
//      .format("delta")
////      .bucketBy(10, "AccountNo")  // Bucket by AccountNo//doesnt support bucketby
//      .save(delPath)
////      .save("/home/avyuthan-shah/Desktop/F1Internstatus.writeFile("true")/DETasksSet#2/DeltaLake/Hadoop/dataF")

//     val startTime=System.nanoTime()

    df.write
      .format("delta")
      .mode("overwrite")
//      .option("overwriteSchema","true")//Schema Evolvement in New Version removing old data
      .option("optimizeWrite", "true")
      .option("autoCompact", "true")
//      .partitionBy("AccountNo")//partition by to optimize query for filtering based on AccountNo or you can use by time, date or according to use
      .save(delPathHadoop)

//     val endTime = System.nanoTime()

//     status.writeFile("false")
//     val elapsedTime = (endTime - startTime) / 1e9 // Time in seconds
//     println()
//     println(s"Elapsed time to load from csv 5 million data: $elapsedTime")

    spark.stop()
  }
}
