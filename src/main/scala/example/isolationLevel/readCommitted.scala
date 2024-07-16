package example.isolationLevel

import example.Extra.time
import org.apache.spark.sql.SparkSession
import io.delta.tables.DeltaTable

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._


object readCommitted extends App{
  implicit val ec: ExecutionContext = ExecutionContext.global
  val spark = SparkSession.builder()
    .appName("SerializableIsolation")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  val deltaPath = "/home/avyuthan-shah/Desktop/dataF"

  def readCommit(spark: SparkSession): Future[Unit] = Future {
    println("Read Committed started")
    spark.read.format("delta").load(deltaPath).createOrReplaceTempView("deltaTable")

    val df = spark.sql(
      """
        |SELECT * FROM deltaTable
        |WHERE AccountNo="337777"
      """.stripMargin)
    df.show()
    println("Read Committed completed")
  }

  def writeOperation(spark: SparkSession): Future[Unit] = Future {
    println("Write operation started")
    val deltaTable = DeltaTable.forPath(spark, deltaPath)
    deltaTable.as("dt").updateExpr(
      "AccountNo = '337777' ",
      Map("DEPOSITAMT" -> "DEPOSITAMT + 100.0", "BALANCEAMT"->"BALANCEAMT + 100.0", "VALUEDATE"->s"to_date('${time.getTime()}', 'yy-MM-dd')")
    )
    println("Write operation completed")
  }

  // Perform concurrent read and write operations
  val writeFuture = writeOperation(spark)
  val readFuture = readCommit(spark)


  Await.result(Future.sequence(Seq(readFuture, writeFuture)), Duration.Inf)
  spark.stop()

}
