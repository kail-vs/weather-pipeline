# Weather Stream Processing with Kafka, Spark, PostgreSQL, and pgAdmin

## Overview
This project is a real-time weather data streaming pipeline built with **Apache Kafka**, **Apache Spark Structured Streaming**, and **PostgreSQL** for persistent storage.  
The system ingests synthetic weather data, processes it in real-time with Spark, and stores both raw and aggregated results in a PostgreSQL database.  
A **pgAdmin** web interface is included for easy database management.

---

## Features
- **Real-time ingestion** of weather data from Kafka.
- **Structured Streaming** with Spark to clean, enrich, and aggregate data.
- **PostgreSQL integration** for storing processed results.
- **pgAdmin 4** for visual database access and management.
- **Docker Compose** setup for easy deployment of all services.
- **Batch-level logging** via Spark’s StreamingQueryListener.

---

## Repository Structure
```
.
├── configs/                      # Configuration files
├── data/                         # Local data directory (if needed later)
├── producer/                     # Kafka weather data producer code
│   └── Dockerfile
├── spark/                        # Spark streaming job code
│   ├── weather_stream_processor.py
│   └── Dockerfile
├── docker-compose.yml            # Orchestration for Kafka, Spark, Postgres, pgAdmin
├── .env                          # Environment variables (DB credentials, etc.)
├── README.md                     # Project documentation
└── requirements.txt              # Python dependencies for producer
```

---

## 🛠 Services Overview
### **1. Kafka + Zookeeper**
- Kafka broker for message streaming.
- Zookeeper for Kafka coordination.

### **2. Spark**
- **Master node** and two **workers** for distributed streaming processing.
- Runs the `weather_stream_processor.py` job to read Kafka, process data, and store results in PostgreSQL.

### **3. PostgreSQL**
- Stores cleaned and aggregated weather data in tables.

### **4. pgAdmin**
- Web UI to query, manage, and inspect PostgreSQL data.

---

## Configuration

### **Environment Variables (`.env`)**
Example:
```
POSTGRES_USER=weatheruser
POSTGRES_PASSWORD=weatherpass
POSTGRES_DB=weatherdb
PGADMIN_DEFAULT_EMAIL=admin@admin.com
PGADMIN_DEFAULT_PASSWORD=admin
```

---

## Running the Project
### Clone the repository
```bash
git clone <repo_url>
cd <repo_name>
```

### Start services with Docker Compose
```bash
docker compose up -d
```

### Check logs
```bash
docker logs -f spark-job
```

---

## Accessing the Services
- **Kafka**: Internal connection only (`kafka:9092`)
- **Spark Master UI**: [http://localhost:8080](http://localhost:8080)
- **Spark Worker 1 UI**: [http://localhost:8081](http://localhost:8081)
- **Spark Worker 2 UI**: [http://localhost:8082](http://localhost:8082)
- **pgAdmin**: [http://localhost:5050](http://localhost:5050)

---

## PostgreSQL Access

### **From Terminal**
```bash
docker exec -it postgres psql -U weatheruser -d weatherdb
```

### **List Tables**
```sql
\dt
```

### **Query Data**
```sql
SELECT * FROM weather_data LIMIT 10;
SELECT * FROM weather_aggregates LIMIT 10;
```

---

## Development Notes
- Spark job writes **only to PostgreSQL**, no filesystem storage.
- PostgreSQL tables are created automatically by the job if they do not exist.
- The system is designed for **append-only** streaming mode.

---

