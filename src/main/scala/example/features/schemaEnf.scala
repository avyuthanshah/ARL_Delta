package example.features

import example.Extra.{time,status}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object schemaEnf extends App{
  val spark = SparkSession.builder()
    .appName("Schema Enforcement")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")//to automerge schema
    .getOrCreate()

  val delPath = "/home/avyuthan-shah/Desktop/Data/dataFFF"

  import spark.implicits._

  val newData = Seq(
    ("107777",time.getDate(), "Withdraw", 3, time.getDate(), "0.0", 300.0, 3600.0,time.getTime()),
    ("117777", time.getDate(), "Deposit", 3, time.getDate(),"200.0", 0.0, 5000.0,time.getTime())
  )
    .toDF("AccountNo","DATE","TRANSACTIONDETAILS", "CHQNO", "VALUEDATE", "WITHDRAWALAMT", "DEPOSITAMT", "BALANCEAMT","TIME")//Status is the new column and it can be enforced by setting mergeSchema option to True -1 is for negative or loan balance amt and 1 for positive or no loan
//    .withColumn("DATE", to_date($"DATE","yy-MM-dd"))
    .withColumn("VALUEDATE", to_date($"VALUEDATE","yy-MM-dd"))
    .withColumn("TIME", to_timestamp($"TIME", "HH:mm:ss"))

  newData.show()

  status.writeFile("true")
  Thread.sleep(100)//delay for monitoring
  val deltaT=DeltaTable.forPath(spark,delPath)

  try {
    deltaT.as("dt")
      .merge(
        newData.as("nd"),"dt.AccountNo=nd.AccountNo AND dt.VALUEDATE=nd.VALUEDATE"
      )
      .whenMatched().updateAll()
      .whenNotMatched().insertAll()
      .execute()
    //Schema Evolution using option("mergeSchema",true)
//    val startTime=System.nanoTime()
//    newData.write
//      .format("delta")
//      .mode("append")
//      // .option("mergeSchema","true")
//      .save(delPath)
//    val endTime = System.nanoTime()
//
//    val elapsedTime = (endTime - startTime) / 1e9 // Time in seconds
//    println()
//    println(s"Elapsed time for schema enf: $elapsedTime")
//    println()
  }catch {
    case e:Exception=>
      println()
      println(s"Error Occured: ${e.getMessage}")
      println()
  }
/*
The merge operation in Delta Lake, as shown in the above code, does not automatically update the schema of the Delta table. The merge operation is primarily used for upserts, allowing to conditionally insert, update, or delete records based on specified conditions.
When using the merge operation to merge data with a Delta table, it assumes that the schema of the incoming data (additionalData) matches the schema of the Delta table (oldData). If there are new columns in the incoming data that are not present in the Delta table, they will be ignored during the merge operation.
To handle schema evolution, one needs to explicitly specify to merge the schema of the incoming data with the schema of the Delta table. It can be achieved by setting the mergeSchema option to true when writing the new data to the Delta table.
 */
  status.writeFile("false")

  // Perform compaction to optimize storage
  spark.sql(s"OPTIMIZE delta.`$delPath`")//compaction to optimize partition

  val deltaT2=DeltaTable.forPath(spark,delPath)
  deltaT2.toDF.filter(col("AccountNo").isin("107777","117777")).show(truncate=false)
  spark.stop()

}
