package example
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import example.Extra.status

object json2del extends App{
  val spark = SparkSession.builder()
    .appName("Json to Delta")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  //Schema should have same header name as specifies in json
  val schema = StructType(Array(
    StructField("Account No", StringType, nullable = false),
    StructField("DATE", StringType, nullable = false),
    StructField("TRANSACTION DETAILS", StringType, nullable = false),
    StructField("CHQ NO", StringType, nullable = true),
    StructField("VALUE DATE", StringType, nullable = false),
    StructField("WITHDRAWAL AMT", StringType, nullable = true),
    StructField("DEPOSIT AMT", StringType, nullable = true),
    StructField("BALANCE AMT", StringType, nullable = true)
  ))

  import spark.implicits._
  val df= spark.read
    .option("multiline", "true")//for multiple records
//    .option("treatEmptyValuesAsNulls", "true")
//    .option("inferSchema", "false")
    .schema(schema)//must define schema to read json file correctly
    .json("/home/avyuthan-shah/Desktop/F1Intern/Datasets/Bank_transaction/bank.json")


  // Rename columns to remove spaces
  val renamedDF = df.columns.foldLeft(df) { (tempDF, colName) =>
    tempDF.withColumnRenamed(colName, colName.replaceAll(" ", ""))
  }

  val filtered_df = renamedDF
    .withColumn("DATE", to_date($"DATE", "dd-MM-yy"))
    .withColumn("VALUEDATE", to_date($"VALUEDATE", "dd-MM-yy"))
    .withColumn("CHQNO", when($"CHQNO" === "nan", null).otherwise($"CHQNO"))
    .withColumn("WITHDRAWALAMT", when($"WITHDRAWALAMT" === "nan", null).otherwise($"WITHDRAWALAMT").cast(DoubleType))
    .withColumn("DEPOSITAMT", when($"DEPOSITAMT" === "nan", null).otherwise($"DEPOSITAMT").cast(DoubleType))
    .withColumn("BALANCEAMT", when($"BALANCEAMT" === "nan", null).otherwise($"BALANCEAMT").cast(DoubleType))
  //.withColumn("WITHDRAWAL AMT", $"WITHDRAWAL AMT".cast(DoubleType))
  //.withColumn("DEPOSIT AMT", $"DEPOSIT AMT".cast(DoubleType))
  //.withColumn("BALANCE AMT", $"BALANCE AMT".cast(DoubleType))

  filtered_df.show()

  val repartitioned_df = filtered_df.repartition(4)

  status.writeFile("true")
  Thread.sleep(100)

  val startTime = System.nanoTime()
  repartitioned_df.write
    .mode("overwrite")
    .format("delta")
    .save("/home/avyuthan-shah/Desktop/Data/dataF_json")
  val endTime = System.nanoTime()

  status.writeFile("false")


  val elapsedTime = (endTime - startTime) / 1e9 // Time in seconds
  println()
  println(s"Elapsed Time to upload into delta from json : $elapsedTime")
  println()

  spark.stop()
}
