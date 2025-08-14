FROM bitnami/spark:3.5.1

USER 0

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY spark/weather_stream_processor.py .
COPY configs ./configs
COPY .env .

CMD ["/opt/bitnami/spark/bin/spark-submit", \
     "--master", "spark://spark-master:7077", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", \
     "/app/weather_stream_processor.py"]
