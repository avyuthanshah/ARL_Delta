package example.Extra

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object getBal extends App{
//  val delPath = "/home/avyuthan-shah/Desktop/dataF"

  def updTab(spark: SparkSession,delPath:String, accNo: String, addBal: Double): Unit = {  //Updates single latest record for given accountnumber

    spark.read.format("delta").option("header", "true").load(delPath).createOrReplaceTempView("delta_table")

    val result = spark.sql(
      s"""
         |SELECT BALANCEAMT FROM delta_table
         |WHERE AccountNo = $accNo
         |ORDER BY VALUEDATE DESC,TIME DESC
         |LIMIT 1;
         |""".stripMargin)

    val balance = result.head.getAs[Double]("BALANCEAMT")

    val dT = DeltaTable.forPath(spark, delPath)
    dT.update(
      condition = col("AccountNo") === accNo && col("BALANCEAMT") === balance,
      set = Map("DEPOSITAMT" -> lit(balance+addBal), "BALANCEAMT" -> lit(balance + addBal))
    )
    dT.toDF.filter(col("AccountNo") === accNo).show()
  }

  def getTabBal(spark: SparkSession,delPath:String, accNo: String): Double = { //Retrieve latest balance for given account number

    spark.read.format("delta").option("header", "true").load(delPath).createOrReplaceTempView("delta_table")

    val result = spark.sql(
      s"""
         |SELECT BALANCEAMT FROM delta_table
         |WHERE AccountNo = "$accNo"
         |ORDER BY VALUEDATE DESC,TIME DESC
         |LIMIT 1;
         |""".stripMargin)

    val balance = result.head.getAs[Double]("BALANCEAMT")

    balance

//    println(s"Current balance for account $accNo: $balance")
  }
}
