import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, expr, window, avg, count, round
)
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.streaming import StreamingQueryListener

# ====================================================
# Kafka Config
# ====================================================
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "weather-data"

# ====================================================
# Output Directories (ensure they exist)
# ====================================================
OUTPUT_DIR = "/tmp/output"         # ← Change this if you want another folder
CHECKPOINT_DIR = "/tmp/checkpoints"  # ← Change checkpoint location if needed

os.makedirs(OUTPUT_DIR, mode=0o777, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, mode=0o777, exist_ok=True)

# ====================================================
# Schema for Incoming JSON data
# ====================================================
schema = (
    StructType()
    .add("city", StringType())
    .add("temperature", DoubleType())
    .add("humidity", DoubleType())
    .add("weather", StringType())
    .add("timestamp", TimestampType())
)

# ====================================================
# Spark Session
# ====================================================
spark = (
    SparkSession.builder
    .appName("WeatherStreamProcessorAdvanced")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ====================================================
# Listener for Streaming Progress
# ====================================================
class BatchLogger(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"[INFO] Streaming Query Started: {event.id}")

    def onQueryProgress(self, event):
        print(f"[INFO] Batch {event.progress.batchId} | Rows: {event.progress.numInputRows}")

    def onQueryTerminated(self, event):
        print(f"[WARN] Streaming Query Terminated: {event.id}")

spark.streams.addListener(BatchLogger())

# ====================================================
# Read from Kafka
# ====================================================
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")  # ← Change to "earliest" if you want all stored Kafka messages
    .option("failOnDataLoss", "false")
    .load()
)

# ====================================================
# Parse JSON & Clean Data
# ====================================================
json_df = (
    df.selectExpr("CAST(value AS STRING) as json_str")
    .withColumn("data", from_json(col("json_str"), schema))
    .select("data.*")
)

# Drop rows missing critical fields
clean_df = json_df.na.drop(subset=["city", "temperature", "humidity", "timestamp"])

# Add derived columns
enriched_df = clean_df.withColumn(
    "temperature_status",
    expr(
        "CASE WHEN temperature > 35 THEN 'HOT' "
        "WHEN temperature < 10 THEN 'COLD' "
        "ELSE 'MODERATE' END"
    )
).withColumn(
    "heat_index",
    round(
        col("temperature") + (col("humidity") / 100) * col("temperature") * 0.1,
        2
    )
)

# ====================================================
# Windowed Aggregation
# ====================================================
# NOTE: You can change "2 minutes" to a smaller/larger value if you want quicker/slower output.
agg_df = (
    enriched_df
    .withWatermark("timestamp", "5 seconds")  # ← Watermark delay; reduce for faster flush
    .groupBy(
        window(col("timestamp"), "5 seconds"),  # ← Window size; reduce to e.g. "30 seconds" for faster output
        col("city")
    )
    .agg(
        avg("temperature").alias("avg_temp"),
        avg("humidity").alias("avg_humidity"),
        count("*").alias("reading_count")
    )
)

# ====================================================
# Output Streams
# ====================================================

# 1. Console Output — good for debugging
console_query = (
    enriched_df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .start()
)

# 2. Aggregated JSON Output — skip empty batches
def write_non_empty_batches(df, batch_id):
    """
    Writes non-empty microbatches to JSON.
    Change this function if you want another format (e.g., CSV, Parquet).
    """
    if df.count() > 0:  # Prevent creation of empty files
        df.write.mode("append").json(OUTPUT_DIR)

agg_query = (
    agg_df.writeStream
    .foreachBatch(write_non_empty_batches)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .outputMode("append")
    .trigger(processingTime="5 seconds") 
    .start()
)

# ====================================================
# Keep Streaming Alive
# ====================================================
spark.streams.awaitAnyTermination()
