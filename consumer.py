"""Kafka Weather AI Consumer mit konfigurierbaren KI-Providern."""

import json
import logging
import os
from datetime import datetime, timezone
from time import perf_counter
from urllib.parse import quote_plus

import httpx
import ollama
import openai
from confluent_kafka import Consumer, KafkaException, Producer
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from ai.base import AIProvider
from ai.settings import (
    create_provider_from_settings,
    load_ai_settings,
    settings_identity,
)
from ai_cache import (
    get_cached_analysis,
    store_cached_analysis,
    weather_fingerprint,
)
from pipeline_resilience import (
    InvalidWeatherRecord,
    message_already_processed,
    retry_operation,
    send_to_dlq,
    validate_weather,
)

load_dotenv(override=True)

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "127.0.0.1:9092")
KAFKA_TOPIC = "weather_data"
KAFKA_GROUP = "weather-ai-consumer"
MONGO_DATABASE = "weather_project"
MONGO_COLLECTION = "weather_analysis"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


def mongo_uri_from_env() -> str:
    """Lädt MongoDB-Zugangsdaten erst beim Start, nicht beim Import."""
    username = quote_plus(os.environ["MONGO_USERNAME"])
    password = quote_plus(os.environ["MONGO_PASSWORD"])
    host = os.getenv("MONGO_HOST", "localhost")
    port = os.getenv("MONGO_PORT", "27017")
    return f"mongodb://{username}:{password}@{host}:{port}/?authSource=admin"


# ==========================================
# KI-Antwortformat und Prompt
# ==========================================


class Activity(BaseModel):
    name: str
    description: str
    reason: str


class WeatherAnalysis(BaseModel):
    sentiment_score: int = Field(ge=1, le=10)
    sentiment_label: str
    summary: str
    activities: list[Activity] = Field(min_length=3, max_length=3)


# ==========================================
# Ollama-Auswertung
# ==========================================


def analyze_weather(
    weather: dict,
    provider: AIProvider,
) -> WeatherAnalysis:
    """Analysiert Wetterdaten über den konfigurierten KI-Anbieter."""

    prompt = f"""
Analysiere die folgenden Wetterdaten als Familienassistent.

WETTERDATEN:
{json.dumps(weather, ensure_ascii=False, indent=2)}

DEINE AUFGABEN:

1. Bewerte das Wetter mit einem Sentiment-Score von 1 bis 10.
   1 = sehr ungeeignet für Aktivitäten draußen
   5 = mittelmäßig
   10 = ausgezeichnet für Aktivitäten draußen

2. Vergib eine kurze deutsche Bewertung als sentiment_label.

3. Verfasse eine kurze, hilfreiche Wetterzusammenfassung.

4. Empfiehl GENAU 3 konkrete Outdoor-Aktivitäten
   für zwei Eltern mit einer kleinen Tochter.

REGELN:
- Berücksichtige Temperatur, Regenwahrscheinlichkeit,
  Windgeschwindigkeit und Wettercode.
- Achte auf altersgerechte, familienfreundliche Vorschläge.
- Bevorzuge bei Bedarf kurze, geschützte Ausflüge,
  die auch mit einem Kinderwagen möglich sind.
- Bei schlechtem Wetter keine riskanten Aktivitäten empfehlen.
- Begründe jede Aktivität anhand der Wetterlage.
- Erfinde keine zusätzlichen Wetterdaten oder lokalen Angebote.
- Beachte: Regenwahrscheinlichkeit ist kein gemessener Regen.
- Antworte ausschließlich auf Deutsch.
- Gib deine Antwort im vorgegebenen JSON-Format zurück.
"""

    analysis = provider.generate(
        system_prompt=(
            "Du bist ein hilfreicher, vorsichtiger "
            "Familien- und Wetterassistent. "
            "Bewerte Wetterbedingungen sachlich und "
            "empfiehl sichere Aktivitäten für Familien."
        ),
        user_prompt=prompt,
        response_model=WeatherAnalysis,
    )

    return analysis


# ==========================================
# MongoDB-Speicherung
# ==========================================


def save_to_mongodb(
    collection,
    weather: dict,
    analysis: WeatherAnalysis,
    message,
    provider: AIProvider,
    model_identity: str,
) -> None:
    """Speichert eine Wetterbeobachtung idempotent mit korrekter KI-Metadatenkennung."""
    document_id = f"{message.topic()}:{message.partition()}:{message.offset()}"

    document = {
        "_id": document_id,
        "city": weather.get("city"),
        "weather": weather,
        "ai_analysis": analysis.model_dump(),
        "metadata": {
            "model": provider.model_name,
            "provider": provider.provider_name,
            "model_identity": model_identity,
            "kafka_topic": message.topic(),
            "kafka_partition": message.partition(),
            "kafka_offset": message.offset(),
            "processed_at": datetime.now(timezone.utc),
        },
    }

    try:
        collection.insert_one(document)
    except DuplicateKeyError:
        logger.info("Nachricht %s bereits gespeichert", document_id)


# ==========================================
# Kafka-Consumer
# ==========================================


def run_consumer() -> None:
    """Validiert Kafka-Nachrichten, nutzt KI-Cache und committed erst nach Speicherung."""
    logger.info("Starte Kafka Weather AI Consumer")

    mongo_client = None
    consumer = None
    dlq_producer = None

    try:
        mongo_client = MongoClient(mongo_uri_from_env(), serverSelectionTimeoutMS=10000)
        mongo_client.admin.command("ping")
        db = mongo_client[MONGO_DATABASE]
        collection = db[MONGO_COLLECTION]
        cache_collection = db["ai_analysis_cache"]
        logger.info("MongoDB verbunden")

        consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_SERVER,
                "group.id": KAFKA_GROUP,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "max.poll.interval.ms": 900000,
            }
        )
        dlq_producer = Producer(
            {
                "bootstrap.servers": KAFKA_SERVER,
                "acks": "all",
                "enable.idempotence": True,
            }
        )
        consumer.subscribe([KAFKA_TOPIC])
        logger.info("Warte auf Nachrichten aus Topic '%s' ...", KAFKA_TOPIC)

        cache_hits = 0
        cache_misses = 0
        active_settings_id = None
        ai_provider = None

        while True:
            message = consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())

            # Bestehende Kafka-Events weder erneut analysieren noch cachen.
            if message_already_processed(collection, message):
                logger.info(
                    "SKIP | Nachricht %s:%s:%s bereits verarbeitet",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )
                consumer.commit(message=message, asynchronous=False)
                continue

            try:
                weather = validate_weather(message.value())
            except InvalidWeatherRecord as error:
                logger.warning("Ungültige Wetterdaten: %s", error)
                # Erst eine erfolgreiche DLQ-Zustellung, dann den Offset bestätigen.
                send_to_dlq(dlq_producer, message, str(error))
                consumer.commit(message=message, asynchronous=False)
                continue

            logger.info("Verarbeite Wetterdaten für %s", weather.get("city"))

            # Einstellungen vor jeder neuen Analyse laden. Kein Neustart nötig.
            settings = load_ai_settings(db)
            new_settings_id = settings_identity(settings)
            if ai_provider is None or new_settings_id != active_settings_id:
                ai_provider = create_provider_from_settings(settings)
                active_settings_id = new_settings_id
                logger.info(
                    "KI-Konfiguration aktiviert: %s | %s",
                    ai_provider.provider_name,
                    ai_provider.model_name,
                )

            # Provider, Modell und Endpoint gehen in die Cache-Identität ein.
            model_identity = active_settings_id
            fingerprint = weather_fingerprint(weather, model_identity)
            cached_analysis = get_cached_analysis(cache_collection, fingerprint)

            if cached_analysis is not None:
                analysis = WeatherAnalysis.model_validate(cached_analysis)
                cache_hits += 1
                cache_status = "hit"
                logger.info(
                    "AI Cache HIT | %s | Fingerprint: %s",
                    weather["city"],
                    fingerprint[:12],
                )
            else:
                cache_status = "miss"
                logger.info(
                    "AI Cache MISS | %s | Starte %s (%s)",
                    weather["city"],
                    ai_provider.model_name,
                    ai_provider.provider_name,
                )
                start_time = perf_counter()
                analysis = retry_operation(
                    lambda: analyze_weather(weather, ai_provider),
                    attempts=3,
                    initial_delay=2,
                    retry_exceptions=(
                        ConnectionError,
                        TimeoutError,
                        httpx.TimeoutException,
                        ollama.ResponseError,
                        openai.APIConnectionError,
                        openai.APITimeoutError,
                        openai.RateLimitError,
                        openai.InternalServerError,
                    ),
                )
                logger.info(
                    "KI-Auswertung abgeschlossen in %.2f Sekunden",
                    perf_counter() - start_time,
                )
                store_cached_analysis(
                    cache_collection,
                    fingerprint,
                    model_identity,
                    analysis.model_dump(),
                )
                cache_misses += 1

            # Pro Nachricht genau eine Speicherung und ein Kafka-Commit.
            save_to_mongodb(
                collection,
                weather,
                analysis,
                message,
                ai_provider,
                model_identity,
            )
            consumer.commit(message=message, asynchronous=False)

            total = cache_hits + cache_misses
            logger.info(
                "AI Cache | Treffer: %s | Misses: %s | Hit Rate: %.1f%% | Letzter: %s",
                cache_hits,
                cache_misses,
                cache_hits / total * 100 if total else 0,
                cache_status,
            )
            logger.info(
                "Nachricht erfolgreich verarbeitet: Partition %s, Offset %s",
                message.partition(),
                message.offset(),
            )

    except KeyboardInterrupt:
        logger.info("Consumer wird beendet ...")
    except Exception:
        logger.exception(
            "Verarbeitung fehlgeschlagen. Nachricht bleibt unbestätigt, "
            "Consumer wird beendet."
        )
        raise
    finally:
        if consumer is not None:
            consumer.close()
        if dlq_producer is not None:
            dlq_producer.flush(timeout=15)
        if mongo_client is not None:
            mongo_client.close()
        logger.info("Verbindungen geschlossen")


if __name__ == "__main__":
    run_consumer()
