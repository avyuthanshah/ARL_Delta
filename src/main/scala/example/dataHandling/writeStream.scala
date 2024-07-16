package example.dataHandling
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object writeStream extends App{
  val spark = SparkSession.builder()
    .appName("write to delta")
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

  val df = spark.readStream
    .schema(schema)
    .option("header", "false")
    .csv("/home/avyuthan-shah/Desktop/F1Intern/Datasets/Bank_transaction/StreamingBank")

  import spark.implicits._
  val filtered_df=df
    .withColumn("DATE",to_date($"DATE", "dd-MM-yy"))
    .withColumn("VALUEDATE", to_date($"VALUEDATE", "dd-MM-yy"))

  val query=filtered_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkPointLocation","/home/avyuthan-shah/Desktop/dataFcheckpoints")
    //Checkpoints are essential in structured streaming with Spark because they store the state of the streaming query.
    // This allows Spark to recover from failures and continue processing from where it left off rather than starting from scratch.
    // The checkpoint directory is used to persist metadata about the streaming job, such as offsets and progress.
    .start("/home/avyuthan-shah/Desktop/dataF")

  query.awaitTermination()
}
