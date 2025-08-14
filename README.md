# Weather Streaming Processor (Advanced)

A **real-time weather data processing pipeline** built with **Apache Kafka** and **Apache Spark Structured Streaming**.  
This application consumes weather data from Kafka, performs advanced processing (cleaning, enrichment, aggregation), and writes processed results to output storage in JSON format.

---

## Features

### 1. **Real-time Data Ingestion**
- Connects to a Kafka topic (`weather-data`).
- Reads JSON-formatted weather events containing:
  - `city`
  - `temperature`
  - `humidity`
  - `weather`
  - `timestamp` (ISO format)

### 2. **Data Parsing & Cleaning**
- Parses JSON from Kafka messages.
- Drops records with missing values in key fields (`city`, `temperature`, `humidity`, `timestamp`).
- Converts `timestamp` into proper Spark `TimestampType` for time-based operations.

### 3. **Data Enrichment**
- Adds **`temperature_status`**:
  - `HOT` if temperature > 35°C
  - `COLD` if temperature < 10°C
  - `MODERATE` otherwise
- Calculates **`heat_index`** using temperature and humidity.

### 4. **Windowed Aggregations**
- Applies **event-time windowing** (`2 minutes` by default).
- Uses **watermarking** (`2 minutes`) to handle late data.
- Calculates per-city metrics:
  - Average temperature
  - Average humidity
  - Count of readings

### 5. **Batch Progress Logging**
- Custom `StreamingQueryListener` logs:
  - Query start events
  - Batch progress (batch ID + rows processed)
  - Query termination events

### 6. **Output Modes**
- **Console Output**: Real-time enriched data for debugging.
- **File Output**:
  - Writes aggregated results to `/tmp/output` in JSON format.
  - Uses `foreachBatch` to skip writing empty batches (avoids empty files).
  - Saves results without unwanted Spark metadata files.
- **Checkpointing**:
  - Uses `/tmp/checkpoints` for Spark checkpointing to maintain state.

---

## Project Structure
```
weather-stream-processor/
│
├── spark/
│   ├── weather_stream_processor.py  # Main Spark Structured Streaming app
│
├── kafka/                           # Kafka broker & topic setup (if using Docker)
│
├── README.md                        # Project documentation
└── requirements.txt                  # Python dependencies
```

---

## Configuration
### Kafka
Update Kafka broker and topic in the Python script if needed:
```python
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "weather-data"
```

### Output & Checkpoints
- **Output directory**: `/tmp/output`
- **Checkpoint directory**: `/tmp/checkpoints`

Make sure these directories are writable from the environment where Spark runs.

---

## Running the Application

1. **Start Kafka** (e.g., via Docker Compose or your local cluster)
2. **Produce sample messages** to the Kafka topic:
```json
{"city": "London", "temperature": 22.5, "humidity": 55, "weather": "Clear", "timestamp": "2025-08-15T12:30:00"}
```
3. **Run the Spark Streaming app**:
```bash
spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0   spark/weather_stream_processor.py
```

---

## Future Improvements
- **Database Sink**: Write results directly to PostgreSQL or Cassandra.
- **Alerting System**: Trigger alerts for extreme temperatures.
- **Real-time Dashboard**: Integrate with tools like Apache Superset or Grafana.
- **Schema Registry**: Use Confluent Schema Registry for robust message schema management.

---

## License
MIT License
