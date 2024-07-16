package example.features

import example.Extra.time
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object consistency {
  def main(args:Array[String]):Unit= {
    val spark = SparkSession.builder()
      .appName("Consistency Check In Delta")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val delPath = "/home/avyuthan-shah/Desktop/dataF"

    def getBalance(id: String):Double = {
      spark.read.format("delta").option("header","true").load(delPath).createOrReplaceTempView("delta_table")

      val result = spark.sql(s"""
      SELECT BALANCEAMT FROM delta_table
      WHERE AccountNo="$id" ORDER BY VALUEDATE DESC LIMIT 1;
      """)

      //result.show(truncate=false)
      val firstRow = result.head // Assuming there's at least one row
      val balance = firstRow.getAs[Double]("BALANCEAMT")

      balance
    }

    def updateData(b1:Double,b2:Double):Unit={

      import spark.implicits._
      val newData = Seq(
        ("409000611074'",time.getTime(), "Withdrawn", "Null", time.getTime(), 1000.0, 0.0, b1-1000),
        ("447777", time.getTime(), "Deposit", "Null", time.getTime(), 0.0, 1000.0, b2+1000)
      )
        .toDF("AccountNo", "DATE", "TRANSACTIONDETAILS", "CHQNO", "VALUEDATE", "WITHDRAWALAMT", "DEPOSITAMT", "BALANCEAMT")
        .withColumn("DATE", to_date($"DATE", "yy-MM-dd"))
        .withColumn("VALUEDATE", to_date($"VALUEDATE", "yy-MM-dd"))

      try {

        newData.write.format("delta").mode("append").save(delPath)
        println("Data written to Delta Lake successfully.")

      } catch {
        case e: Exception => println("Failed to write data to Delta Lake: " + e.getMessage)
      }

    }

    //before updating
    val b1:Double= getBalance("409000611074'")
    val b2:Double=getBalance("447777")
    val total=b1+b2

    updateData(b1,b2)

    val b12:Double= getBalance("409000611074'")
    val b22:Double=getBalance("447777")
    val total2=b12+b22

    println()
    println(b1,b2)
    println(b12,b22)
    println(s"Total Amount Before Transaction:$total")
    println(s"Total Amount Before Transaction:$total2")
    println()

    spark.stop()
  }
}
