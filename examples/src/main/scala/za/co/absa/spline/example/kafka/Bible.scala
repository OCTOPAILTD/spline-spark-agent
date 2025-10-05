package za.co.absa.spline.example.batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object KafkaBibleStream {

  def main(args: Array[String]): Unit = {

    // Step 1: Start Spark session
    val spark = SparkSession.builder()
      .appName("ScalaKafkaBibleStream")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Step 2: Define schema
    val schema = new StructType()
      .add("chapter", StringType)
      .add("verse", StringType)
      .add("text", StringType)

    try {
      // Step 3: Read from Kafka
      val kafkaRaw = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "192.168.1.102:9093")
        .option("subscribe", "bible")
        .load()

      // Step 4: Convert JSON in value column
      val valueDF = kafkaRaw
        .selectExpr("CAST(value AS STRING) AS json")
        .select(from_json($"json", schema).alias("data"))
        .select("data.*")

      // Step 5: Count "God" in text
      val withGodCount = valueDF.withColumn("GodCount", size(split($"text", "God")) - 1)

      // Step 6: Filter verses with more than 1 "God"
      val filteredGod = withGodCount.filter($"GodCount" > 1)

      // Step 7: Filter verses containing "and"
      val filteredAnd = filteredGod.filter($"text".contains("and"))

      // Step 8: Add word count column
      val withWordCount = filteredAnd.withColumn("Counter_words", size(split($"text", " ")))

      // Step 9: Categorize verse length
      val categorized = withWordCount.withColumn("length",
        when($"Counter_words" > 14, "long")
          .when($"Counter_words" > 7 && $"Counter_words" <= 14, "medium")
          .otherwise("small")
      )

      // Step 10: Drop GodCount column
      val finalResult = categorized.drop("GodCount")

      // Step 11: Write to Parquet
      val query = finalResult.writeStream
        .format("parquet")
        .option("path", "/tmp/kafka_output/parquet")
        .option("checkpointLocation", "/tmp/kafka_output/checkpoint")
        .outputMode("append")
        .start()

      query.awaitTermination()
    }
    catch {
      case ex: Exception =>
        println(s"Error: ${ex.getMessage}")
        ex.printStackTrace()
    }
  }
}
