# Real-Time Weather Streaming Pipeline + Streamlit Dashboard

End‑to‑end, Dockerized pipeline that ingests live weather data, processes it with Apache Spark Structured Streaming, persists aggregates in PostgreSQL, and serves a real‑time dashboard with Streamlit. Optional pgAdmin is included for easy DB inspection.

---

## ✨ What you get

- **Kafka + Spark**: Robust streaming ingestion and windowed aggregations (watermarks, tumbling windows)
- **PostgreSQL**: Durable storage of aggregated weather metrics (`weather_agg` table)
- **Streamlit dashboard**: Auto‑refresh UI with per‑city filters and interactive charts
- **pgAdmin (optional)**: Web UI to browse/query the database
- **One command up**: Everything runs via `docker compose`

---

## 🧭 Architecture

```
┌────────────────────┐        ┌─────────────┐        ┌─────────────────────┐
│  Weather Producer  │  --->  │   Kafka     │  --->  │  Spark Structured   │
│ (API -> topic)     │        │  (broker)   │        │   Streaming (agg)   │
└────────────────────┘        └─────────────┘        └─────────┬───────────┘
                                                               │
                                                               ▼
                                                       ┌──────────────┐
                                                       │  PostgreSQL  │
                                                       │  weather\_agg│
                                                       └───────┬──────┘
                                                               │
                                                               ▼
                                                       ┌──────────────┐
                                                       │  Streamlit   │
                                                       │  Dashboard   │
                                                       └──────────────┘
```
---

## Repository structure

```

.
├── docker-compose.yml
├── .env
├── producer/
│   ├── Dockerfile
│   └── weather_producer.py
├── spark/
│   ├── Dockerfile
│   └── weather_stream_processor.py
├── dashboard/
│   ├── Dockerfile
│   └── app.py
├── configs/
│   └── cities.json
├── data/                          
└── requirements.txt              

````

---

## Database schema

Spark writes aggregated rows to `weather_agg`:

| Column          | Type        | Meaning                             |
|-----------------|-------------|-------------------------------------|
| `batch_time`    | TIMESTAMPTZ | Processing batch time (window end)  |
| `city`          | TEXT        | City name                           |
| `avg_temp`      | DOUBLE      | Avg temperature for the window      |
| `avg_humidity`  | DOUBLE      | Avg humidity for the window         |
| `reading_count` | BIGINT      | Number of raw records in window     |

> The Spark job will create/append to this table automatically (idempotent upserts avoided by window granularity).



---

## Configuration

Create a `.env` at the repo root (used by services):

```ini
# PostgreSQL
POSTGRES_DB=weatherdb
POSTGRES_USER=weatheruser
POSTGRES_PASSWORD=weatherpass
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

# Producer (if using external API keys)
OPENWEATHER_API_KEY=your_openweather_api_key
FETCH_INTERVAL=60
CITY_LIMIT=10

# Streamlit
STREAMLIT_SERVER_PORT=8501
STREAMLIT_BROWSER_GATHERUSAGESTATS=false
````

> `POSTGRES_HOST=postgres` is the Docker service name from `docker-compose.yml`, not `localhost`.

---

## Run

Build and start:

```bash
docker compose up --build
```

Services & ports (default):

* **Streamlit**: [http://localhost:8501](http://localhost:8501)
* **PostgreSQL**: localhost:5432 (inside compose network as `postgres:5432`)
* **pgAdmin (optional)**: [http://localhost:5050](http://localhost:5050)
* **Kafka broker**: `kafka:9092` (internal)

Stop:

```bash
docker compose down
```

Stop and remove volumes (reset DB):

```bash
docker compose down -v
```

---

## Dashboard (Streamlit)

The dashboard auto-refreshes and shows:

* **Global counters** (latest batch)
* **City filter** (`All` or a specific city)
* **Temperature trend** (line, per city)
* **Humidity trend** (line, per city)
* **Reading counts** (bar, latest batch or rolling window)
* **Raw aggregates table** (paged, sortable)

Open: **[http://localhost:8501](http://localhost:8501)**

> If the page shows “No data yet”, ensure the producer is sending messages and the Spark job is running. Data appears as soon as the first aggregation window closes.

---

## Postgres & pgAdmin

### psql (from host)

```bash
PGPASSWORD=weatherpass psql -h localhost -U weatheruser -d weatherdb -p 5432
```

Quick checks:

```sql
SELECT * FROM weather_agg ORDER BY batch_time DESC LIMIT 20;

SELECT city, COUNT(*) AS windows
FROM weather_agg
GROUP BY city
ORDER BY windows DESC;
```

### pgAdmin (web UI)

* Open [http://localhost:5050](http://localhost:5050)
* Login with pgAdmin credentials you set in `docker-compose.yml` (e.g., `PGADMIN_DEFAULT_EMAIL`, `PGADMIN_DEFAULT_PASSWORD`)
* Add a server:

  * **Host**: `postgres`
  * **Port**: `5432`
  * **Maintenance DB**: `weatherdb`
  * **Username**: `weatheruser`
  * **Password**: `weatherpass`

---

## Spark job highlights

* **Kafka source** with `startingOffsets=latest`
* **Event time**: expects `timestamp` in the payload; cast to `TimestampType`
* **Watermarking**: drops late data older than the watermark (configurable)
* **Windowed aggregation**: tumbling windows (e.g., 60s / 2m; tune as needed)
* **Exactly‑once sinks**: per‑batch DB writes via `foreachBatch` (transactional)

> If you reduce window size aggressively (e.g., 5s), ensure your producer cadence matches, or you’ll see sparse windows.

---

## Troubleshooting

**Dashboard says “relation does not exist”**
The Spark job hasn’t written to `weather_agg` yet. Wait for the first window to close or verify Spark logs.

**pgAdmin “password authentication failed”**
Make sure you are using the DB user/password (`weatheruser` / `weatherpass`), not the pgAdmin web login.

**No data in charts**
Confirm:

* Producer logs show messages produced to Kafka
* Spark logs show batches with `numInputRows > 0`
* `weather_agg` has rows (`SELECT COUNT(*) FROM weather_agg;`)

**Connecting from host to Postgres**
Use `localhost:5432`. Inside containers, use `postgres:5432`.

---

## Tech Stack

* **Apache Kafka** (wurstmeister images)
* **Apache Spark 3.5.1** (Bitnami)
* **PostgreSQL**
* **Streamlit + Plotly**
* **Docker Compose**
* **Python: pandas, psycopg2, kafka-python**

---

## Branching

* `phase1` — earlier file-based sink iteration
* `spark-processing` — spark integration with advance stream processing
* `postgres-integration` — offloading output data to postgres database
* `main` — current default branch with Postgres + dashboard

> To switch default branch on GitHub: **Settings → Branches → Default branch**.

---

## License

MIT — do what you like; attribution appreciated.

---

## Credits

Maintained by **Kail**. Contributions welcome!

