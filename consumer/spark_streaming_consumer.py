from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
from pyspark.sql.functions import from_json, col

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "raw_sensor_readings"
BRONZE_PATH = "../data/bronze"
CHECKPOINT_PATH = "../data/checkpoints/bronze_writer"

# Schema harus eksplisit didefinisikan — Spark gak bisa nebak struktur JSON
# dari stream secara otomatis (beda dengan baca file batch biasa)
READING_SCHEMA = StructType([
    StructField("location_name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("ingested_at", StringType(), True),
    StructField("temperature_c", DoubleType(), True),
    StructField("humidity_pct", IntegerType(), True),
    StructField("wind_speed_kmh", DoubleType(), True),
    StructField("precipitation_mm", DoubleType(), True),
])


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("IoTBronzeIngestion")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("INFO")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Kafka selalu kasih value sebagai bytes; kita cast ke string lalu parse JSON
    # sesuai schema yang udah didefinisikan di atas
    parsed_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), READING_SCHEMA).alias("data"))
        .select("data.*")
    )

    query = (
        parsed_stream.writeStream
        .format("parquet")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime="60 seconds")
        .start()
    )

    print("Streaming consumer started. Writing to Bronze layer every 30s.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
