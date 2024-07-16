package example.isolationLevel

import example.Extra.{time, status}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import io.delta.tables.DeltaTable

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object concWrite2 extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global
  val spark = SparkSession.builder()
    .appName("ConcurrentWrite")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  val deltaPath = "/home/avyuthan-shah/Desktop/Data/dataFF"

  def writeOperation(spark: SparkSession, accountNo: String, depBal: Double): Future[Unit] = Future {
    val latestBalance = example.Extra.getBal.getTabBal(spark, deltaPath, accountNo) // Gets the latest balance of the given account number
    val deltaTable = DeltaTable.forPath(spark, deltaPath)
    val bal=500.0+depBal

    val currTime: String = time.getTime()
    val currDate:String=time.getDate()
    import spark.implicits._
    val newDf = Seq(
      (accountNo, currDate, "Deposit", "Null", currDate, 0.0, depBal, bal,currTime)
    ).toDF("AccountNo", "DATE", "TRANSACTIONDETAILS", "CHQNO", "VALUEDATE", "WITHDRAWALAMT", "DEPOSITAMT", "BALANCEAMT","TIME")
      .withColumn("DATE", to_date($"DATE", "yy-MM-dd"))
      .withColumn("VALUEDATE", to_date($"VALUEDATE", "yy-MM-dd"))

    val cols = newDf.columns
    val mergeCondition = cols.map(col => s"dt.$col=df.$col").mkString(" AND ")

    deltaTable.as("dt")
      .merge(newDf.as("df"), s"$mergeCondition AND dt.AccountNo= '$accountNo' AND dt.TIME='$currTime'")
      .whenMatched().updateAll()
      .whenNotMatched().insertAll()
      .execute()
//    newDf.write.format("delta").mode("append").save(deltaPath)

    println(s"Write operation for account $accountNo completed successfully")
  }

  status.writeFile("true")

  val writeFutures = Seq(
    writeOperation(spark, "88104", 100.0),
//    writeOperation(spark, "10000", 200.0),
    writeOperation(spark, "50000", 500.0)
  )

  //  88104
  //  28192
  //  20012
  val combinedFuture = Future.sequence(writeFutures)

  combinedFuture.onComplete {
    case Success(_) =>
      println("All write operations completed successfully")
      spark.stop()
    case Failure(e) =>
      println(s"One or more write operations failed: ${e.getMessage}")
      spark.stop()
  }

  Await.result(combinedFuture, Duration.Inf)

  status.writeFile("false")
  spark.stop()
}
