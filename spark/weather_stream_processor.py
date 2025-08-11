import os
import time
from kafka import KafkaAdminClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql.streaming import StreamingQueryListener

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

wait_for_topic("kafka:9092", "weather-data", timeout=120)

schema = StructType() \
    .add("city", StringType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("weather", StringType()) \
    .add("timestamp", StringType())

spark = SparkSession.builder \
    .appName("WeatherStreamProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

class BatchLogger(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Streaming Query Started: {event.id}")
    def onQueryProgress(self, event):
        batch_id = event.progress.batchId
        rows = event.progress.numInputRows
        print(f"Batch {batch_id} processed with {rows} rows.")
    def onQueryTerminated(self, event):
        print(f"Streaming Query Terminated: {event.id}")

spark.streams.addListener(BatchLogger())

print(f"Waiting for data from Kafka topic: {KAFKA_TOPIC}")
while True:
    static_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    if static_df.count() > 0:
        print("Data found in Kafka. Starting streaming query...")
        break
    else:
        print("No data yet. Retrying in 5 seconds...")
        time.sleep(5)

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), schema)) \
    .select("data.*")

result_df = json_df.withColumn(
    "temperature_status",
    expr("""
        CASE
            WHEN temperature > 35 THEN 'HOT'
            WHEN temperature < 10 THEN 'COLD'
            ELSE 'MODERATE'
        END
    """)
)

console_query = result_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

json_query = result_df.writeStream \
    .format("json") \
    .option("path", OUTPUT_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()
