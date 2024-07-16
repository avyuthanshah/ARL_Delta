package example.features

import example.Extra.status
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Encoders, SparkSession}

object fetch extends App{
  val spark=SparkSession.builder()
    .appName("Fetch Delta Table")
    .master("local[*]")//runs on localhost 4040
    //.config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  //val delPath="hdfs://localhost:9000/delta/dataF"
  val delPath="/home/avyuthan-shah/Desktop/Data/dataFF"
 // val version=DeltaTable.forPath(spark, delPath).history(1).select("version").as[Long](Encoders.scalaLong).head //Time Travel Feature can be accessed by mentioning version of table in option
  // Register the Delta table as a temporary view
  //.option("versionAsOf",version)
  status.writeFile("true")
//  Thread.sleep(200)

  val startTime = System.nanoTime()
  spark.read.format("delta").option("versionAsOf",2).load(delPath)createOrReplaceTempView("delta_table")
  // Run SQL queries on the Delta table
//  val result = spark.sql(
//  s"""
//      |SELECT * FROM delta_table
//      |WHERE AccountNo IN ("107777","409000611074")
//      |ORDER BY AccountNo;
//      """.stripMargin)
//  val result = spark.sql(
//    s"""
//       |SELECT * FROM delta_table
//       |WHERE AccountNo IN ("8321928","3526295","7135748","3223026","6701229")
//       |ORDER BY AccountNo,VALUEDATE DESC;
//      """.stripMargin)

  val result = spark.sql(
    s"""
       |SELECT * FROM delta_table
       |WHERE AccountNo IN ("56135","69310","65260")
       |ORDER BY AccountNo,VALUEDATE DESC,TIME DESC;
      """.stripMargin)

//  val result = spark.sql(
//    s"""
//       |SELECT * FROM delta_table
//       |WHERE CHQNO IS NOT NULL;
//      """.stripMargin)

//  val result = spark.sql(
//    s"""
//       |SELECT AccountNo,COUNT(*) AS count FROM delta_table
//       |GROUP BY AccountNo
//       |HAVING AccountNo IN ("409000611074'","1196428'");
//      """.stripMargin)

  val endTime = System.nanoTime()
  println(status.readFile())
  result.show(truncate=false)

  status.writeFile("false")

  //Using dataframe
//  val startTime2 = System.nanoTime()
//  val df:DataFrame=spark.read
//    .format("delta")
//    .option("header","true")
//    .option("versionAsOf",version)
//    .option("treatEmptyValueAsNulls","true")
//    .load(delPath)
//
//  val filterdf = df
//    .filter(col("AccountNo").isin("557777", "667777"))
//    .orderBy(col("VALUEDATE").desc)
//
//  val endTime2 = System.nanoTime()
//  val elapsedTime2 = (endTime2 - startTime2) / 1e9 // Time in seconds
//
//  filterdf.show(truncate=false)
//
//  println()
  val elapsedTime = (endTime - startTime) / 1e9 // Time in seconds
  println(s"Elapsed time for retrival: '$elapsedTime' ")
//  println(s"Elapsed time for query using dataframe method: '$elapsedTime2' ")
//  print(s"${time.getTime()}")
//  println()
  spark.stop()
}

