import json
import logging
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from pydantic import BaseModel, Field, ValidationError

logger = logging.getLogger(__name__)

DLQ_TOPIC = "weather_data_dlq"


class WeatherRecord(BaseModel):
    city: str = Field(min_length=1)
    temperature_c: float = Field(ge=-90, le=60)
    rain_probability_percent: float = Field(ge=0, le=100)
    wind_speed_kmh: float = Field(ge=0, le=500)
    weather_code: int = Field(ge=0, le=99)


class InvalidWeatherRecord(ValueError):
    """Die Kafka-Nachricht enthält ungültige Wetterdaten."""


def validate_weather(payload: bytes) -> dict:
    """Dekodiert und validiert eine Wetter-Nachricht."""
    try:
        data = json.loads(payload.decode("utf-8"))

        if not isinstance(data, dict):
            raise ValueError("JSON muss ein Objekt sein.")

        WeatherRecord.model_validate(data)

        # Zusätzliche Felder wie location_id erhalten
        return data

    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        ValidationError,
        ValueError,
        TypeError,
    ) as error:
        raise InvalidWeatherRecord(str(error)) from error


def retry_operation(
    operation,
    *,
    attempts=3,
    initial_delay=2,
    retry_exceptions=(ConnectionError, TimeoutError),
    sleep=time.sleep,
):
    """Wiederholt eine Operation mit exponentiellem Backoff."""
    if attempts < 1:
        raise ValueError("attempts muss mindestens 1 sein.")

    for attempt in range(1, attempts + 1):
        try:
            return operation()

        except retry_exceptions as error:
            if attempt == attempts:
                logger.error(
                    "Operation nach %s Versuchen gescheitert: %s",
                    attempts,
                    error,
                )
                raise

            delay = initial_delay * (2 ** (attempt - 1))

            logger.warning(
                "Versuch %s/%s fehlgeschlagen: %s. Erneuter Versuch in %s Sekunden.",
                attempt,
                attempts,
                error,
                delay,
            )

            sleep(delay)


def send_to_dlq(
    producer: Producer,
    message,
    reason: str,
    timeout: int = 15,
) -> None:
    """Übermittelt eine ungültige Nachricht an das DLQ."""
    delivered = []
    errors = []

    def delivery_callback(error, record):
        if error:
            errors.append(error)
        else:
            delivered.append(True)

    envelope = {
        "source_topic": message.topic(),
        "source_partition": message.partition(),
        "source_offset": message.offset(),
        "reason": reason,
        "raw_payload": message.value().decode("utf-8", errors="replace"),
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }

    producer.produce(
        topic=DLQ_TOPIC,
        key=message.key(),
        value=json.dumps(envelope, ensure_ascii=False).encode("utf-8"),
        callback=delivery_callback,
    )

    remaining = producer.flush(timeout)

    if remaining > 0 or errors or not delivered:
        raise RuntimeError("DLQ-Nachricht konnte nicht bestätigt werden.")

    logger.warning(
        "Ungültige Nachricht an %s übertragen: Partition %s, Offset %s",
        DLQ_TOPIC,
        message.partition(),
        message.offset(),
    )
