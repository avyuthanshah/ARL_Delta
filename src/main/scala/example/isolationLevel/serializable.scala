package example.isolationLevel

import example.Extra.time
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import io.delta.tables.DeltaTable

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

object serializable extends App{
  implicit val ec: ExecutionContext = ExecutionContext.global
  val spark = SparkSession.builder()
    .appName("SerializableIsolation")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  val deltaPath = "/home/avyuthan-shah/Desktop/dataF"

  def writeOperation(spark:SparkSession):Future[Unit]=Future{
    println()
    println("Write Started")
    println()
    val dT=DeltaTable.forPath(spark,deltaPath)
    dT.updateExpr(
      s"""
         |AccountNo="337777"
      """.stripMargin,
      Map("DEPOSITAMT" -> "DEPOSITAMT + 100.0","BALANCEAMT" -> "BALANCEAMT + 100.0","VALUEDATE"->s"to_date('${time.getTime()}', 'yy-MM-dd')")
    )
    dT.toDF.filter(col("AccountNo")==="337777").show()
    println("Write Completed")
    println()
  }

  def readOperation(spark: SparkSession): Future[Unit] = Future {
    println()
    println("Read operation started")
    println()

    Await.result(writeOperation(spark), Duration.Inf) // Wait for the completion of the serializable write operation

    val df = spark.read.format("delta").load(deltaPath)
    df.filter(col("AccountNo")==="337777").show()
    println("Read operation completed")
    println()
  }

  val readFuture = readOperation(spark)
  Await.result(readFuture, Duration.Inf)
  spark.stop()
}
