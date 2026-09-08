import json
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

# Lokasi yang mau dipantau: (nama, latitude, longitude)
LOCATIONS = [
    ("Depok", -6.4025, 106.7942),
    ("Jakarta", -6.2088, 106.8456),
]

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "raw_sensor_readings"
POLL_INTERVAL_SECONDS = 60


def fetch_weather(lat: float, lon: float) -> dict:
    """Ambil data cuaca terkini dari Open-Meteo API untuk satu koordinat."""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def build_reading(location_name: str, lat: float, lon: float, raw: dict) -> dict:
    """Bentuk payload sensor reading yang konsisten, siap dikirim ke Kafka."""
    current = raw.get("current", {})
    return {
        "location_name": location_name,
        "latitude": lat,
        "longitude": lon,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "temperature_c": current.get("temperature_2m"),
        "humidity_pct": current.get("relative_humidity_2m"),
        "wind_speed_kmh": current.get("wind_speed_10m"),
        "precipitation_mm": current.get("precipitation"),
    }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Producer started. Polling every {POLL_INTERVAL_SECONDS}s. Ctrl+C to stop.")

    try:
        while True:
            for name, lat, lon in LOCATIONS:
                try:
                    raw = fetch_weather(lat, lon)
                    reading = build_reading(name, lat, lon, raw)
                    producer.send(KAFKA_TOPIC, value=reading)
                    print(f"Sent: {reading}")
                except requests.RequestException as e:
                    print(f"Failed to fetch weather for {name}: {e}")

            producer.flush()
            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
