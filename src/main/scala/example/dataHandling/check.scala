package example.dataHandling
import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object check extends App{
    val kafkaBootstrapServers = "localhost:9092"
    val kafkaTopic = "mydB.mydB.bank116k"
    
    val spark = SparkSession.builder
    .appName("KafkaDeltaIntegration")
    .master("local[*]")
    .getOrCreate()

    // Assuming your Kafka stream is already set up and running
    val kafkaStream = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaBootstrapServers)
    .option("subscribe", kafkaTopic)
    .option("auto.offset.reset", "latest")
    .load()

    import spark.implicits._

    // Print raw JSON data to inspect the structure
    val rawJsonStream = kafkaStream.selectExpr("CAST(value AS STRING) as json")

    rawJsonStream.writeStream
    .format("console")
    .start()
    .awaitTermination()
}