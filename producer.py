"""Publish weather observations for saved locations every interval."""

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone
from urllib.parse import quote_plus

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from pymongo import MongoClient

from location_service import list_tracked_locations

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
TOPIC = "weather_data"


def mongo_uri() -> str:
    user = quote_plus(os.environ["MONGO_USERNAME"])
    password = quote_plus(os.environ["MONGO_PASSWORD"])
    host = os.getenv("MONGO_HOST", "127.0.0.1")
    port = os.getenv("MONGO_PORT", "27017")
    return f"mongodb://{user}:{password}@{host}:{port}/?authSource=admin"


def get_weather(location: dict) -> dict:
    """Current weather + probability for current UTC hour."""
    response = requests.get(
        WEATHER_URL,
        params={
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "current": "temperature_2m,wind_speed_10m,weather_code",
            "hourly": "precipitation_probability",
            "forecast_days": 2,
            "timezone": "UTC",
        },
        timeout=15,
    )
    response.raise_for_status()
    data = response.json()
    current = data["current"]
    hourly = data["hourly"]
    hour = current["time"][:13]
    index = next(
        (i for i, value in enumerate(hourly["time"]) if value[:13] == hour), None
    )
    if index is None:
        raise ValueError("No hourly precipitation forecast for current conditions")
    return {
        "location_id": location["_id"],
        "city": location["name"],
        "country": location.get("country", ""),
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "weather_time": current["time"] + "Z",
        "temperature_c": current["temperature_2m"],
        "rain_probability_percent": hourly["precipitation_probability"][index],
        "wind_speed_kmh": current["wind_speed_10m"],
        "weather_code": current["weather_code"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="WeatherWatcher multi-city producer")
    parser.add_argument(
        "--once", action="store_true", help="Run one poll cycle and exit"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=int(os.getenv("WEATHER_INTERVAL_SECONDS", "300")),
    )
    args = parser.parse_args()
    if args.interval < 30:
        parser.error("--interval must be at least 30 seconds")

    with MongoClient(mongo_uri(), serverSelectionTimeoutMS=10000) as mongo:
        db = mongo[os.getenv("MONGO_DATABASE", "weather_project")]
        mongo.admin.command("ping")
        producer = KafkaProducer(
            bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP_SERVER", "127.0.0.1:9092")],
            value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode(
                "utf-8"
            ),
            key_serializer=lambda value: value.encode("utf-8"),
            acks="all",
            retries=3,
        )
        try:
            while True:
                locations = list_tracked_locations(db)
                if not locations:
                    logger.warning("No tracked locations. Add one in the dashboard.")
                for location in locations:
                    try:
                        weather = get_weather(location)
                        metadata = producer.send(
                            TOPIC,
                            key=str(location["_id"]),
                            value=weather,
                        ).get(timeout=20)
                        logger.info(
                            "Published %s (%s) offset=%s",
                            location["name"],
                            location["_id"],
                            metadata.offset,
                        )
                    except Exception:
                        logger.exception("Failed to publish %s", location.get("name"))
                if args.once:
                    break
                time.sleep(args.interval)
        finally:
            producer.flush(timeout=20)
            producer.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Producer stopped")
