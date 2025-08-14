import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window, avg, count, round
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.streaming import StreamingQueryListener

# PostgreSQL Config from environment variables
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "weatherdb")
DB_USER = os.getenv("DB_USER", "weatheruser")
DB_PASS = os.getenv("DB_PASS", "weatherpass")

# Kafka Config
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "weather-data"

schema = (
    StructType()
    .add("city", StringType())
    .add("temperature", DoubleType())
    .add("humidity", DoubleType())
    .add("weather", StringType())
    .add("timestamp", TimestampType())
)

spark = (
    SparkSession.builder
    .appName("WeatherStreamProcessorDB")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

class BatchLogger(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"[INFO] Streaming Query Started: {event.id}")
    def onQueryProgress(self, event):
        print(f"[INFO] Batch {event.progress.batchId} | Rows: {event.progress.numInputRows}")
    def onQueryTerminated(self, event):
        print(f"[WARN] Streaming Query Terminated: {event.id}")

spark.streams.addListener(BatchLogger())

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

json_df = (
    df.selectExpr("CAST(value AS STRING) as json_str")
    .withColumn("data", from_json(col("json_str"), schema))
    .select("data.*")
)

clean_df = json_df.na.drop(subset=["city", "temperature", "humidity", "timestamp"])

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

agg_df = (
    enriched_df
    .withWatermark("timestamp", "5 seconds")
    .groupBy(
        window(col("timestamp"), "5 seconds"),
        col("city")
    )
    .agg(
        avg("temperature").alias("avg_temp"),
        avg("humidity").alias("avg_humidity"),
        count("*").alias("reading_count")
    )
)

def write_to_postgres(df, batch_id):
    rows = df.collect()
    if not rows:
        return
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_agg (
            batch_time TIMESTAMP,
            city TEXT,
            avg_temp DOUBLE PRECISION,
            avg_humidity DOUBLE PRECISION,
            reading_count INTEGER
        )
    """)
    for row in rows:
        cur.execute("""
            INSERT INTO weather_agg (batch_time, city, avg_temp, avg_humidity, reading_count)
            VALUES (%s, %s, %s, %s, %s)
        """, (row['window'].start, row['city'], row['avg_temp'], row['avg_humidity'], row['reading_count']))
    conn.commit()
    cur.close()
    conn.close()

console_query = (
    enriched_df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .start()
)

agg_query = (
    agg_df.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("update")
    .trigger(processingTime="5 seconds")
    .start()
)

spark.streams.awaitAnyTermination()
