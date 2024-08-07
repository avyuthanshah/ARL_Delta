package example.dataHandling

import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import io.delta.tables.DeltaTable

object kafkaStream extends App{
    val kafkaBootstrapServers = "localhost:9092"
    val kafkaTopic = "mydB.mydB.bank116k"

    val delPath="/home/avyuthan-shah/Desktop/Data/dataFF"
    val checkPoint="/home/avyuthan-shah/Desktop/F1Intern/DETasksSet#2/DeltaLake/delta/src/main/scala/example/dataHandling/dataFcheckpoints"

    val spark = SparkSession.builder()
    .appName("write to delta")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.autoCompact.enabled", "true") // Enable auto-compaction globally. By default will use 128 MB as the target file size.
    .config("spark.databricks.delta.optimizeWrite.enabled", "true")
    .config("spark.databricks.delta.autoCompact.targetFileSize", "256MB") // Set target file size for auto-compaction
    .config("spark.databricks.delta.optimizeWrite.binSize", "256MB") // Set bin size for optimized writes
    .getOrCreate()

  //   val schema = StructType(Array(
  //   StructField("AccountNo", StringType, nullable = true),
  //   StructField("DATE", DateType, nullable = true),
  //   StructField("TRANSACTIONDETAILS", StringType, nullable = true),
  //   StructField("CHQNO", StringType, nullable = true),
  //   StructField("VALUEDATE", DateType, nullable = true),
  //   StructField("WITHDRAWALAMT", DoubleType, nullable = true),
  //   StructField("DEPOSITAMT", DoubleType, nullable = true),
  //   StructField("BALANCEAMT", DoubleType, nullable = true),
  //   StructField("TIME", StringType, nullable = true)
  // ))
    val schema = new StructType()
    .add("payload", new StructType()
      .add("after", new StructType()
        .add("AccountNo", StringType, true)
        .add("DATE", IntegerType, true) // DATE is represented as an int (days since epoch)
        .add("TRANSACTIONDETAILS", StringType, true)
        .add("CHQNO", StringType, true)
        .add("VALUEDATE", IntegerType, true) // VALUEDATE is represented as an int (days since epoch)
        .add("WITHDRAWALAMT", DoubleType, true)
        .add("DEPOSITAMT", DoubleType, true)
        .add("BALANCEAMT", DoubleType, true)
        .add("TIME", StringType, true)
      )
    )

  val df = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaBootstrapServers)
  .option("subscribe", kafkaTopic)
  .option("auto.offset.reset", "latest")
  .load()


  val deltaTable = DeltaTable.forPath(spark, delPath)

  val parsedStream = df.selectExpr("CAST(value AS STRING) as json")
  .select(from_json(col("json"), schema).alias("data"))
  .select("data.payload.after.*")

  // Convert DATE and VALUEDATE from int to actual date
  val formattedStream = parsedStream.withColumn("DATE", expr("date_add('1970-01-01', DATE)"))
  .withColumn("VALUEDATE", expr("date_add('1970-01-01', VALUEDATE)"))

  val query = formattedStream
  .writeStream
  .format("delta")
  .outputMode("append")
  .option("checkPointLocation",checkPoint)
  //Checkpoints are essential in structured streaming with Spark because they store the state of the streaming query.
  // This allows Spark to recover from failures and continue processing from where it left off rather than starting from scratch.
  // The checkpoint directory is used to persist metadata about the streaming job, such as offsets and progress.
  .start(delPath)

  query.awaitTermination()

}