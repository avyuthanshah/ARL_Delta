package example
import org.apache.spark.sql.{DataFrame,SparkSession}
//import org.apache.spark.sql.functions._
//import io.delta.tables._

object delt2csv {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder()
      .appName("Delta to Csv")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val df:DataFrame=spark.read.format("delta").option("header","true").option("treatEmptyValuesAsNulls", "true").load("/home/avyuthan-shah/Desktop/Data/dataF")
    df.show()
    val repart=df.repartition(1)
    repart.write.format("csv").mode("overwrite").option("header","true").save("/home/avyuthan-shah/Desktop/Data/conv")


  }
}
