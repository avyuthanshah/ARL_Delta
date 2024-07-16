package example.isolationLevel

import example.Extra.time
import org.apache.spark.sql.{Encoders, SparkSession}
import io.delta.tables.DeltaTable

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

object repeatRead extends App{ //Or Snapshot Isolation in Delta
  implicit val ec: ExecutionContext = ExecutionContext.global
  val spark = SparkSession.builder()
    .appName("Repeatable Read")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  val deltaPath = "/home/avyuthan-shah/Desktop/dataF"

  def readOperation(spark: SparkSession): Future[Unit] = Future {
    println("Repeatable Read started")

    // Get the current version of the Delta table
    val currentVersion = DeltaTable.forPath(spark, deltaPath).history(1).select("version").as[Long](Encoders.scalaLong).head
    spark.read.format("delta").option("versionAsOf",currentVersion).load(deltaPath).createOrReplaceTempView("deltaTable")

    val df = spark.sql(
      s"""
        |SELECT * FROM deltaTable
        |WHERE AccountNo="337777"
    """.stripMargin)
    println()
    df.show()
    println("Read 1 completed")
    println()

    Thread.sleep(2000)

    val df2=spark.sql(
      s"""
         |SELECT * FROM deltaTable
         |WHERE AccountNo="337777"
         |""".stripMargin)
    println()
    df2.show()
    println("Read 2 Completed")
    println("Repeatable Read Completed")
    println()
  }


  def writeOperation(spark: SparkSession): Future[Unit] = Future {
    println()
    println("Write operation started")
    println()

    val deltaTable = DeltaTable.forPath(spark, deltaPath)
    deltaTable.as("dt").updateExpr(
      "AccountNo = '337777'",
      Map("DEPOSITAMT" -> "DEPOSITAMT + 100.0", "BALANCEAMT"->"BALANCEAMT + 100.0", "VALUEDATE"->s"to_date('${time.getTime()}', 'yy-MM-dd')")
    )
    println()
    println("Write operation completed")
    println()
  }


  val readFuture = readOperation(spark)
  val writeFuture = writeOperation(spark)

  Await.result(Future.sequence(Seq(readFuture, writeFuture)), Duration.Inf)
  spark.stop()


}
