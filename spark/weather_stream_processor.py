import os
import time
from kafka.admin import KafkaAdminClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window, avg, max, min, sum as spark_sum
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, TimestampType

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "weather-data"

OUTPUT_PATH = "/data/output/weather"
CHECKPOINT_PATH = "/data/output/checkpoint"

os.makedirs(OUTPUT_PATH, exist_ok=True)
os.makedirs(CHECKPOINT_PATH, exist_ok=True)

def wait_for_topic(bootstrap_servers, topic, timeout=60):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    start_time = time.time()
    while True:
        topics = admin_client.list_topics()
        if topic in topics:
            print(f"Kafka topic '{topic}' found.")
            break
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Topic '{topic}' not found within {timeout} seconds.")
        print(f"Waiting for Kafka topic '{topic}' to be created...")
        time.sleep(5)

wait_for_topic(KAFKA_BROKER, KAFKA_TOPIC, timeout=120)

schema = StructType() \
    .add("city", StringType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("weather", StringType()) \
    .add("timestamp", LongType())  # epoch seconds

spark = SparkSession.builder \
    .appName("WeatherStreamProcessorAdvanced") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), schema)) \
    .select("data.*") \
    .withColumn("event_time", (col("timestamp").cast("timestamp")))  # convert epoch to timestamp

status_df = json_df.withColumn(
    "temperature_status",
    expr("""
        CASE
            WHEN temperature > 35 THEN 'HOT'
            WHEN temperature < 10 THEN 'COLD'
            ELSE 'MODERATE'
        END
    """)
)

windowed_agg_df = status_df \
    .withWatermark("event_time", "15 minutes") \
    .groupBy(
        col("city"),
        window(col("event_time"), "10 minutes", "5 minutes")
    ).agg(
        avg("temperature").alias("avg_temperature"),
        max("temperature").alias("max_temperature"),
        min("temperature").alias("min_temperature"),
        spark_sum(expr("CASE WHEN temperature_status = 'HOT' THEN 1 ELSE 0 END")).alias("hot_count")
    ) \
    .select(
        col("city"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "avg_temperature",
        "max_temperature",
        "min_temperature",
        "hot_count"
    )

parquet_query = windowed_agg_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", OUTPUT_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .partitionBy("city") \
    .start()

console_query = windowed_agg_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

spark.streams.awaitAnyTermination()
