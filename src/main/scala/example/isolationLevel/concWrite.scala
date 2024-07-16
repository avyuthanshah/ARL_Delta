package example.isolationLevel

import example.Extra.{time,status}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import io.delta.tables.DeltaTable

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}



object concWrite extends App{
  implicit val ec: ExecutionContext = ExecutionContext.global
//  val spark = SparkSession.builder()
//    .appName("ConcurrentWrite")
//    .master("local[*]")
//    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
//    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
//    .getOrCreate()
  val spark = SparkSession.builder()
  .appName("Clean Data")
  .master("local[*]")
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  .config("spark.databricks.delta.optimize.repartition.enabled","true")
  .config("spark.databricks.delta.autoCompact.enabled", "true") // Enable auto-compaction globally. By default will use 128 MB as the target file size.
  .config("spark.databricks.delta.optimizeWrite.enabled", "true") // Enable optimized writes globally.
  .config("spark.databricks.delta.autoCompact.targetFileSize", "256MB") // Set target file size for auto-compaction
  .config("spark.databricks.delta.optimizeWrite.binSize", "256MB") // Set bin size for optimized writes
  .getOrCreate()

  val deltaPath = "/home/avyuthan-shah/Desktop/Data/dataFF"


  def writeOperation(spark: SparkSession, accountNo: String,depBal:Double): Future[Unit] = Future {
    var success = false
    var attempt = 0
    val maxRetries = 3
    val delayBetweenRetries = 1000 // milliseconds

    val latest_balance=example.Extra.getBal.getTabBal(spark,deltaPath,accountNo)//gets the latest balance of given account number
    val deltaTable = DeltaTable.forPath(spark, deltaPath)
    import spark.implicits._
    val new_df=Seq(
      (accountNo, time.getDate(), "Deposit", "Null", time.getDate(), 0.0, depBal, latest_balance + depBal,time.getTime())
    ).toDF("AccountNo", "DATE", "TRANSACTIONDETAILS", "CHQNO", "VALUEDATE", "WITHDRAWALAMT", "DEPOSITAMT", "BALANCEAMT","TIME")
      .withColumn("DATE", to_date($"DATE","yy-MM-dd"))
      .withColumn("VALUEDATE", to_date($"VALUEDATE","yy-MM-dd"))

    val cols=new_df.columns
    val mergeCondition=cols.map(col=>s"dt.$col=df.$col").mkString(" AND ")

    while (!success && attempt < maxRetries) {
      attempt += 1
      println()
      println(s"Write operation for account $accountNo started, attempt $attempt")
      println()

      try {
//        deltaTable.as("dt").updateExpr(
//          s"AccountNo = '$accountNo'AND BALANCEAMT='$latest_balance'",
//          Map(
//            "DEPOSITAMT" -> "DEPOSITAMT + 100.0",
//            "BALANCEAMT" -> "BALANCEAMT + 100.0",
//            "DATE" -> s"to_date('${time.getTime()}', 'yy-MM-dd')",
//            "VALUEDATE" -> s"to_date('${time.getTime()}', 'yy-MM-dd')"
//          )
//        )
        deltaTable.as("dt")
          .merge(new_df.as("df"),s"$mergeCondition")//prevent duplicates
        .whenMatched().updateAll()
        .whenNotMatched().insertAll()
        .execute()

        println()
        println(s"Write operation for account $accountNo completed successfully")
        println()
        success = true

      } catch {
        case e: Exception =>
          println()
          println(s"Error in write operation for account $accountNo on attempt $attempt: ${e.getMessage}")
          println()
          if (attempt < maxRetries) {
            println()
            println(s"Retrying after $delayBetweenRetries ms")
            println()
            Thread.sleep(delayBetweenRetries)
          } else {
            println()
            println(s"Max retries reached for account $accountNo")
            println()
            throw e
          }
      }
    }
  }

  status.writeFile("true")

  val writeFutures = Seq(
    writeOperation(spark, "88104", 100.0),
    writeOperation(spark, "10000", 200.0),
    writeOperation(spark, "50000", 500.0)
  )
//  val writeFuture5 = writeOperation(spark, "6701229")


//  8321928
//  3526295
//  7135748
//  3223026
//  6701229
//  5948415
//  9474062
//  6530416
//  3239891
//  8487843

  val combinedFuture = Future.sequence(writeFutures)

  combinedFuture.onComplete {
    case Success(_) =>
      println()
      println("All write operations completed successfully")
      println()
      spark.stop()
    case Failure(e) =>
      println()
      println(s"One or more write operations failed: ${e.getMessage}")
      println()
      spark.stop()
  }

  Await.result(combinedFuture, Duration.Inf)

  status.writeFile("false")
  val deltaTable = DeltaTable.forPath(spark, deltaPath)
  deltaTable.optimize().executeCompaction()
}
