
import argparse
import json
import logging
import time
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

load_dotenv()

# Konfiguration
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
KAFKA_TOPIC = "weather_data"
INTERVAL_SECONDS = 300

GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
WEATHER_URL = "https://api.open-meteo.com/v1/forecast"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def get_coordinates(city: str) -> dict:
    """Ermittelt die Koordinaten einer Stadt."""

    response = requests.get(
        GEOCODING_URL,
        params={
            "name": city,
            "count": 1,
            "language": "de"
        },
        timeout=10
    )
    response.raise_for_status()

    results = response.json().get("results", [])

    if not results:
        raise ValueError(f"Stadt nicht gefunden: {city}")

    location = results[0]

    return {
        "city": location["name"],
        "country": location.get("country", ""),
        "latitude": location["latitude"],
        "longitude": location["longitude"]
    }


def get_weather(location: dict) -> dict:
    """Ruft Wetterdaten von Open-Meteo ab."""

    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "current": (
            "temperature_2m,"
            "wind_speed_10m,"
            "weather_code"
        ),
        "hourly": "precipitation_probability",
        "forecast_days": 1,
        "timezone": "UTC"
    }

    response = requests.get(
        WEATHER_URL,
        params=params,
        timeout=10
    )
    response.raise_for_status()

    data = response.json()

    current = data["current"]
    hourly = data["hourly"]

    # Der aktuelle Zeitpunkt wird auf die Stunde gekürzt.
    # So finden wir die passende Niederschlagsprognose.
    current_hour = current["time"][:13]

    hour_index = next(
        (
            i for i, timestamp in enumerate(hourly["time"])
            if timestamp[:13] == current_hour
        ),
        None
    )

    if hour_index is None:
        raise ValueError(
            "Keine Niederschlagsprognose für die aktuelle Stunde"
        )

    rain_probability = hourly[
        "precipitation_probability"
    ][hour_index]

    return {
        "city": location["city"],
        "country": location["country"],
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "weather_time": current["time"] + "Z",
        "temperature_c": current["temperature_2m"],
        "rain_probability_percent": rain_probability,
        "wind_speed_kmh": current["wind_speed_10m"],
        "weather_code": current["weather_code"]
    }


def create_producer() -> KafkaProducer:
    """Erstellt eine Verbindung zum Kafka-Broker."""

    return KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False
        ).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=3
    )


def run(city: str) -> None:
    """Startet die Wetterdaten-Pipeline."""

    logger.info("Suche Koordinaten für %s", city)

    location = get_coordinates(city)

    logger.info(
        "Stadt gefunden: %s, %s",
        location["city"],
        location["country"]
    )

    producer = create_producer()

    logger.info("Kafka-Verbindung hergestellt")
    logger.info("Topic: %s", KAFKA_TOPIC)

    try:
        while True:
            try:
                weather = get_weather(location)

                # Asynchron senden, auf Bestätigung warten.
                future = producer.send(
                    topic=KAFKA_TOPIC,
                    key=location["city"],
                    value=weather
                )

                metadata = future.get(timeout=15)

                logger.info(
                    "Gesendet | Stadt: %s | Temperatur: %s °C"
                    " | Regenwahrscheinlichkeit: %s %%"
                    " | Wind: %s km/h | Wettercode: %s"
                    " | Partition: %s | Offset: %s",
                    weather["city"],
                    weather["temperature_c"],
                    weather["rain_probability_percent"],
                    weather["wind_speed_kmh"],
                    weather["weather_code"],
                    metadata.partition,
                    metadata.offset
                )

            except (
                requests.RequestException,
                KafkaError,
                ValueError,
                KeyError,
                IndexError
            ) as error:
                logger.error("Fehler: %s", error)

            time.sleep(INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Producer wird beendet ...")

    finally:
        producer.flush()
        producer.close()
        logger.info("Kafka-Verbindung geschlossen")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Open-Meteo Kafka Weather Producer"
    )
    parser.add_argument(
        "--city",
        type=str,
        default="Berlin",
        help="Stadtname, beispielsweise Berlin oder München"
    )

    args = parser.parse_args()

    try:
        run(args.city)
    except (
        requests.RequestException,
        KafkaError,
        ValueError
    ) as error:
        logger.error("Start fehlgeschlagen: %s", error)
        raise SystemExit(1)
