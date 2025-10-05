package src.main.java.za.co.absa.spline.example.batch;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.*;

import static org.apache.spark.sql.functions.*;

public class KafkaBibleStream {

    public static void main(String[] args) throws Exception {

        // Step 1: Start Spark session
        SparkSession spark = SparkSession.builder()
            .appName("JavaKafkaBibleStream")
            .master("local[*]")
            .getOrCreate();

        // Step 2: Define schema
        StructType schema = new StructType()
            .add("chapter", DataTypes.StringType)
            .add("verse", DataTypes.StringType)
            .add("text", DataTypes.StringType);

        // Step 3: Read from Kafka
        try {
            Dataset<Row> kafkaRaw = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "192.168.1.102:9093")
                .option("subscribe", "bible")
                .load();

            // Step 4: Convert JSON in value column
            Dataset<Row> valueDF = kafkaRaw
                .selectExpr("CAST(value AS STRING) as json")
                .select(from_json(col("json"), schema).alias("data"))
                .select("data.*");


                // Step 5: Count "God" in text
                Dataset<Row> withGodCount = valueDF.withColumn("GodCount", size(split(col("text"), "God")).minus(1));

                // Step 6: Filter verses with more than 1 "God"
                Dataset<Row> filteredGod = withGodCount.filter(col("GodCount").gt(1));

                // Step 7: Filter verses containing "and"
                Dataset<Row> filteredAnd = filteredGod.filter(col("text").contains("and"));

                // Step 8: Add word count column
                Dataset<Row> withWordCount = filteredAnd.withColumn("Counter_words", size(split(col("text"), " ")));

                // Step 9: Categorize verse length
                Dataset<Row> categorized = withWordCount.withColumn(
                    "length",
                    functions.when(col("Counter_words").gt(14), "long")
                        .when(col("Counter_words").gt(7).and(col("Counter_words").leq(14)), "medium")
                        .otherwise("small")
                );

                // Step 10: Drop GodCount column
                Dataset<Row> finalResult = categorized.drop("GodCount");

                // Step 11: Write to Parquet
                StreamingQuery query = finalResult.writeStream()
                    .format("parquet")
                    .option("path", "/tmp/kafka_output/parquet")
                    .option("checkpointLocation", "/tmp/kafka_output/checkpoint")
                    .outputMode("append")
                    .start();

                query.awaitTermination();

        }
        catch (Exception name) {
            System.out.println(name);
        }
    }
}
