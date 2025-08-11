# producer/weather_producer.py
import os
import time
import json
import random
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise ValueError("Missing OPENWEATHER_API_KEY in environment")

FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", 120))
CITY_LIMIT = int(os.getenv("CITY_LIMIT", 10))

with open("configs/cities.json", "r") as f:
    city_list = json.load(f)
ALL_CITIES = [c["name"] for c in city_list]

CITIES = random.sample(ALL_CITIES, min(CITY_LIMIT, len(ALL_CITIES)))

print("Producer: waiting 60s for Kafka to start...")
time.sleep(60)

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Producer started. Polling cities: {CITIES}")

while True:
    for city in CITIES:
        try:
            resp = requests.get(
                "https://api.openweathermap.org/data/2.5/weather",
                params={"q": city, "appid": API_KEY, "units": "metric"},
                timeout=15
            )
            resp.raise_for_status()
            data = resp.json()

            message = {
                "city": city,
                "temperature": float(data["main"]["temp"]),
                "humidity": int(data["main"]["humidity"]),
                "weather": data["weather"][0]["main"],
                "timestamp": int(data.get("dt", time.time()))
            }

            producer.send("weather-data", value=message)
            producer.flush()  # ensure message is sent promptly
            print(f"[Produced] {message}")

        except Exception as e:
            print(f"[Error] Failed to fetch/send for {city}: {e}")

    print(f"Producer sleeping for {FETCH_INTERVAL} seconds...")
    time.sleep(FETCH_INTERVAL)
