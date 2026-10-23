
import json
import logging
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from ollama import Client
from pydantic import BaseModel, Field, ValidationError
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from urllib.parse import quote_plus
load_dotenv()

# ==========================================
# Konfiguration
# ==========================================

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
KAFKA_TOPIC = "weather_data"
KAFKA_GROUP = "weather-ai-consumer"

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2")

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
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
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
    activities: list[Activity] = Field(
        min_length=3,
        max_length=3
    )


# ==========================================
# Ollama-Auswertung
# ==========================================

def analyze_weather(
    weather: dict,
    ollama_client: Client
) -> WeatherAnalysis:
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
                )
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        format=WeatherAnalysis.model_json_schema(),
        options={
            "temperature": 0.2
        },
        stream=False
    )

    return WeatherAnalysis.model_validate_json(
        response.message.content
    )


# ==========================================
# MongoDB-Speicherung
# ==========================================

def save_to_mongodb(
    collection,
    weather: dict,
    analysis: WeatherAnalysis,
    message
) -> None:
    """Speichert Wetterdaten und KI-Auswertung."""

    # Kafka-Position als eindeutige Dokument-ID:
    # Bei erneutem Verarbeiten keine doppelten Einträge.
    document_id = (
        f"{message.topic}:"
        f"{message.partition}:"
        f"{message.offset}"
    )

    document = {
        "_id": document_id,
        "city": weather.get("city"),
        "weather": weather,
        "ai_analysis": analysis.model_dump(),
        "metadata": {
            "model": OLLAMA_MODEL,
            "kafka_topic": message.topic,
            "kafka_partition": message.partition,
            "kafka_offset": message.offset,
            "processed_at": datetime.now(timezone.utc)
        }
    }

    try:
        collection.insert_one(document)

    except DuplicateKeyError:
        logger.warning(
            "Nachricht %s bereits gespeichert",
            document_id
        )


# ==========================================
# Kafka-Consumer
# ==========================================

def run_consumer() -> None:
    """Liest Kafka-Nachrichten und verarbeitet sie."""

    logger.info("Starte Kafka Weather AI Consumer")

    ollama_client = Client(
        host=OLLAMA_HOST,
        timeout=120
    )

    mongo_client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000
    )

    consumer = None

    try:
        # MongoDB-Verbindung überprüfen
        mongo_client.admin.command("ping")

        collection = mongo_client[
            MONGO_DATABASE
        ][MONGO_COLLECTION]

        logger.info("MongoDB verbunden")

        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_SERVER],
            group_id=KAFKA_GROUP,

            # Nachrichten erst nach Speicherung bestätigen
            enable_auto_commit=False,

            # Beim ersten Start vorhandene Nachrichten lesen
            auto_offset_reset="earliest",

            # Eine Nachricht pro Poll verarbeiten
            max_poll_records=1,

            # Genügend Zeit für lokale KI-Verarbeitung
            max_poll_interval_ms=900000,

            value_deserializer=lambda message: json.loads(
                message.decode("utf-8")
            )
        )

        logger.info(
            "Warte auf Nachrichten aus Topic '%s' ...",
            KAFKA_TOPIC
        )

        for message in consumer:

            weather = message.value

            logger.info(
                "Wetterdaten empfangen: %s | Offset: %s",
                weather.get("city"),
                message.offset
            )

            # 1. KI-Auswertung
            logger.info("Analysiere mit %s ...", OLLAMA_MODEL)

            analysis = analyze_weather(
                weather,
                ollama_client
            )

            logger.info(
                "Sentiment: %s/10 | %s",
                analysis.sentiment_score,
                analysis.sentiment_label
            )

            # 2. MongoDB-Speicherung
            save_to_mongodb(
                collection,
                weather,
                analysis,
                message
            )

            # 3. Erst nach erfolgreichem Speichern committen
            consumer.commit()

            logger.info(
                "MongoDB gespeichert und Kafka bestätigt"
            )

            for index, activity in enumerate(
                analysis.activities,
                start=1
            ):
                logger.info(
                    "Aktivität %s: %s",
                    index,
                    activity.name
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
            consumer.close(autocommit=False)

        mongo_client.close()
        logger.info("Verbindungen geschlossen")


if __name__ == "__main__":
    run_consumer()
