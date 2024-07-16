package example.Extra
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode

object csv2sql extends App{
  val spark = SparkSession.builder()
    .appName("To sql")
    .master("local[*]")
    .getOrCreate()

  val schema = StructType(Array(
    StructField("Address", StringType, nullable = false),
    StructField("AccountNo", StringType, nullable = false),
    StructField("Type", IntegerType, nullable = false),
    StructField("Amount", DoubleType, nullable = true),
    StructField("Date", DateType, nullable = false),
    StructField("Time", StringType, nullable = false),
    StructField("Remarks", StringType, nullable = true)
  ))
  val new_df=spark.read
    .option("header","true")
    .option("treatEmptyValuesAsNulls","true")
    .option("inferSchema","false")
    .schema(schema)
    .csv("/home/avyuthan-shah/Desktop/F1Intern/Datasets/final_sms_data.csv")

  import spark.implicits._
  val df=new_df
    .withColumn("Date", to_date($"Date","yy-MM-dd"))
    .withColumn("Time", date_format(to_timestamp($"Time", "HH:mm:ss"), "HH:mm:ss"))

  df.show()
  val jdbcUrl = "jdbc:mysql://localhost:3306/mydB"
  val jdbcUsername = "root"
  val jdbcPassword = "$QL007server"

  val jdbcProperties = new java.util.Properties()
  jdbcProperties.setProperty("user", jdbcUsername)
  jdbcProperties.setProperty("password", jdbcPassword)
  jdbcProperties.setProperty("driver", "com.mysql.cj.jdbc.Driver")

  val startTime=System.nanoTime()
  // Write Delta table to MySQL
  df.write.mode(SaveMode.Overwrite)
    .jdbc(jdbcUrl, "rw_1M", jdbcProperties)

  val endTime = System.nanoTime()
  val elapsedTime = (endTime - startTime) / 1e9 // Time in seconds
  println()
  println(s"Elapsed Time to upload into Sql server : $elapsedTime")
  println()
}
