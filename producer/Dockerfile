FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer/weather_producer.py .
COPY .env .
COPY configs/ ./configs/

CMD ["python", "weather_producer.py"]
