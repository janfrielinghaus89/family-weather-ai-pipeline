import json
import logging
import httpx
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Consumer, Producer, KafkaException

from ollama import Client, ResponseError
from pydantic import BaseModel, Field
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from urllib.parse import quote_plus
from config import OLLAMA_HOST, OLLAMA_MODEL
from time import perf_counter

from ai_cache import (
    get_cached_analysis,
    store_cached_analysis,
    weather_fingerprint,
)

from pipeline_resilience import (
    InvalidWeatherRecord,
    validate_weather,
    retry_operation,
    send_to_dlq,
)

load_dotenv()

# ==========================================
# Konfiguration
# ==========================================

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "127.0.0.1:9092")
KAFKA_TOPIC = "weather_data"
KAFKA_GROUP = "weather-ai-consumer"


MONGO_URI = (
    f"mongodb://{quote_plus(os.environ['MONGO_USERNAME'])}:"
    f"{quote_plus(os.environ['MONGO_PASSWORD'])}@"
    f"{os.getenv('MONGO_HOST', 'localhost')}:"
    f"{os.getenv('MONGO_PORT', '27017')}/"
    "?authSource=admin"
)
MONGO_DATABASE = "weather_project"
MONGO_COLLECTION = "weather_analysis"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# ==========================================
# KI-Antwortformat
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


def analyze_weather(weather: dict, ollama_client: Client) -> WeatherAnalysis:
    """Analysiert Wetterdaten mit lokalem Ollama."""

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

    response = ollama_client.chat(
        model=OLLAMA_MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    "Du bist ein hilfreicher, vorsichtiger "
                    "Familien- und Wetterassistent. "
                    "Bewerte Wetterbedingungen sachlich und "
                    "empfiehl sichere Aktivitäten für Familien."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        format=WeatherAnalysis.model_json_schema(),
        options={"temperature": 0.2},
        stream=False,
    )

    return WeatherAnalysis.model_validate_json(response.message.content)


# ==========================================
# MongoDB-Speicherung
# ==========================================


def save_to_mongodb(
    collection, weather: dict, analysis: WeatherAnalysis, message
) -> None:
    """Speichert Wetterdaten und KI-Auswertung."""

    # Kafka-Position als eindeutige Dokument-ID:
    # Bei erneutem Verarbeiten keine doppelten Einträge.
    document_id = f"{message.topic()}:{message.partition()}:{message.offset()}"

    document = {
        "_id": document_id,
        "city": weather.get("city"),
        "weather": weather,
        "ai_analysis": analysis.model_dump(),
        "metadata": {
            "model": OLLAMA_MODEL,
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
# Message check 
# ==========================================
def message_already_processed(collection, message) -> bool:
    """Prüft, ob ein Kafka-Event bereits gespeichert wurde."""

    document_id = (
        f"{message.topic()}:"
        f"{message.partition()}:"
        f"{message.offset()}"
    )

    return collection.find_one(
        {"_id": document_id},
        {"_id": 1},
    ) is not None


# ==========================================
# Kafka-Consumer
# ==========================================


def run_consumer() -> None:
    """Liest Kafka-Nachrichten und verarbeitet sie."""

    logger.info("Starte Kafka Weather AI Consumer")

    logger.info("Ollama Host: %s", OLLAMA_HOST)
    logger.info("Ollama Modell: %s", OLLAMA_MODEL)
    ollama_client = Client(host=OLLAMA_HOST, timeout=120, trust_env=False)

    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)

    consumer = None

    try:
        # MongoDB-Verbindung überprüfen
        mongo_client.admin.command("ping")

        collection = mongo_client[MONGO_DATABASE][MONGO_COLLECTION]

        cache_collection = mongo_client[MONGO_DATABASE]["ai_analysis_cache"]

        cache_hits = 0
        cache_misses = 0

        logger.info("MongoDB verbunden")

        consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_SERVER,
                "group.id": KAFKA_GROUP,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "max.poll.interval.ms": 900000,
            }
        )

        consumer.subscribe([KAFKA_TOPIC])

        logger.info("Warte auf Nachrichten aus Topic '%s' ...", KAFKA_TOPIC)

        while True:
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            # ==========================================
            # Bereits verarbeitete Nachrichten überspringen
            # ==========================================

            if message_already_processed(collection, message):
                logger.info(
                    "SKIP | Nachricht %s:%s:%s bereits verarbeitet",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )

                consumer.commit(
                    message=message,
                    asynchronous=False,
                )
                continue

            # 1. Wetterdaten validieren
            try:
                weather = validate_weather(message.value())

            except InvalidWeatherRecord as error:
                logger.warning("Ungültige Wetterdaten: %s", error)

                # Erst DLQ bestätigen lassen
                send_to_dlq(
                    dlq_producer,
                    message,
                    str(error),
                )

                # Danach ursprünglichen Offset bestätigen
                consumer.commit(
                    message=message,
                    asynchronous=False,
                )
                continue

            logger.info(
                "Verarbeite Wetterdaten für %s",
                weather.get("city"),
            )

            # 2. Ollama mit Retry

            # ==========================================
            # AI Response Cache
            # ==========================================

            fingerprint = weather_fingerprint(
                weather,
                OLLAMA_MODEL,
            )

            cached_analysis = get_cached_analysis(
                cache_collection,
                fingerprint,
            )

            if cached_analysis is not None:
                # Cache-Hit: Kein Aufruf von Ollama nötig
                analysis = WeatherAnalysis.model_validate(cached_analysis)
                cache_hits += 1
                cache_status = "hit"

                logger.info(
                    "AI Cache HIT | %s | Fingerprint: %s",
                    weather["city"],
                    fingerprint[:12],
                )

            else:
                # Cache-Miss: KI wie bisher aufrufen
                
                cache_status = "miss"

                logger.info(
                    "AI Cache MISS | %s | Starte %s",
                    weather["city"],
                    OLLAMA_MODEL,
                )

                start_time = perf_counter()

                analysis = retry_operation(
                    lambda: analyze_weather(
                        weather,
                        ollama_client,
                    ),
                    attempts=3,
                    initial_delay=2,
                    retry_exceptions=(
                        ConnectionError,
                        TimeoutError,
                        httpx.TimeoutException,
                        ResponseError,
                    ),
                )

                inference_seconds = perf_counter() - start_time

                logger.info(
                    "KI-Auswertung abgeschlossen in %.2f Sekunden",
                    inference_seconds,
                )

                store_cached_analysis(
                    cache_collection,
                    fingerprint,
                    OLLAMA_MODEL,
                    analysis.model_dump(),
                )

                cache_misses += 1

            # ==========================================
            # Bestehende MongoDB-Speicherung
            # ==========================================

            save_to_mongodb(
                collection,
                weather,
                analysis,
                message,
            )

            consumer.commit(
                message=message,
                asynchronous=False,
            )

            total = cache_hits + cache_misses
            hit_rate = cache_hits / total * 100 if total > 0 else 0

            logger.info(
                "AI Cache | Treffer: %s | Misses: %s | Hit Rate: %.1f%% | Letzter: %s",
                cache_hits,
                cache_misses,
                hit_rate,
                cache_status,
            )

            # 3. MongoDB-Speicherung
            # Vorhandene idempotente Funktion weiterverwenden
            save_to_mongodb(
                collection,
                weather,
                analysis,
                message,
            )

            # 4. Kafka erst nach MongoDB bestätigen
            consumer.commit(
                message=message,
                asynchronous=False,
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
            "Verarbeitung fehlgeschlagen. "
            "Nachricht bleibt unbestätigt, "
            "Consumer wird beendet."
        )
        raise

    finally:
        if consumer is not None:
            consumer.close()

        dlq_producer.flush(timeout=15)
        mongo_client.close()
        logger.info("Verbindungen geschlossen")


dlq_producer = Producer(
    {
        "bootstrap.servers": KAFKA_SERVER,
        "acks": "all",
        "enable.idempotence": True,
    }
)


if __name__ == "__main__":
    run_consumer()
