
import os
from datetime import datetime, timezone
from html import escape
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

import streamlit as st
from pymongo import MongoClient
from pymongo.errors import PyMongoError
load_dotenv()

# ==========================================
# Konfiguration
# ==========================================

MONGO_URI = (
    f"mongodb://{quote_plus(os.environ['MONGO_USERNAME'])}:"
    f"{quote_plus(os.environ['MONGO_PASSWORD'])}@"
    f"{os.getenv('MONGO_HOST', 'localhost')}:"
    f"{os.getenv('MONGO_PORT', '27017')}/"
    "?authSource=admin"
)

DATABASE = "weather_project"
COLLECTION = "weather_analysis"


st.set_page_config(
    page_title="Familienwetter",
    page_icon="🌤️",
    layout="wide"
)


# ==========================================
# MongoDB
# ==========================================

@st.cache_resource
def get_database():
    """MongoDB-Verbindung für Streamlit wiederverwenden."""
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000
    )
    client.admin.command("ping")
    return client[DATABASE]


def load_latest_weather():
    """Den zuletzt verarbeiteten Datensatz laden."""
    db = get_database()

    return db[COLLECTION].find_one(
        sort=[("metadata.processed_at", -1)]
    )


# ==========================================
# Darstellung
# ==========================================

def format_number(value, suffix=""):
    """Zahlen im deutschen Format darstellen."""
    if value is None:
        return "–"

    try:
        number = f"{float(value):.1f}".replace(".", ",")
        return f"{number}{suffix}"
    except (TypeError, ValueError):
        return "–"


def get_sentiment_style(score):
    """Farbe und Bezeichnung des Sentiments bestimmen."""
    if score > 7:
        return "#166534", "#dcfce7", "Sehr gutes Wetter"

    if score >= 5:
        return "#854d0e", "#fef9c3", "Durchschnittliches Wetter"

    return "#991b1b", "#fee2e2", "Ungünstiges Wetter"


# ==========================================
# Header
# ==========================================

st.title("🌤️ Familienwetter")
st.caption(
    "KI-gestützte Wetteranalyse mit "
    "Open-Meteo · Apache Kafka · Ollama · MongoDB"
)

header_left, header_right = st.columns(
    [4, 1],
    vertical_alignment="center"
)

with header_left:
    st.subheader("Aktuelle Wetterübersicht")

with header_right:
    refresh = st.button(
        "🔄 Daten aktualisieren",
        use_container_width=True,
        type="primary"
    )


# ==========================================
# Daten abrufen
# ==========================================

try:
    # Die Abfrage ist nicht gecacht.
    # Jeder Buttonklick führt eine neue Ausführung aus.
    document = load_latest_weather()

except PyMongoError as error:
    st.error(f"MongoDB ist nicht erreichbar: {error}")
    st.stop()

if not document:
    st.warning(
        "Noch keine Wetteranalysen vorhanden. "
        "Bitte Producer und Consumer starten."
    )
    st.stop()


weather = document.get("weather") or {}
analysis = document.get("ai_analysis") or {}
metadata = document.get("metadata") or {}

city = document.get("city") or weather.get("city") or "Unbekannt"
score = analysis.get("sentiment_score", 0)
summary = analysis.get("summary") or "Keine Zusammenfassung vorhanden."
activities = analysis.get("activities") or []

try:
    score = int(score)
except (ValueError, TypeError):
    score = 0

score_color, score_background, score_label = (
    get_sentiment_style(score)
)


# ==========================================
# Wetterübersicht
# ==========================================

st.markdown(f"### 📍 {escape(str(city))}")

temperature = weather.get("temperature_c")
rain = weather.get("rain_probability_percent")
wind = weather.get("wind_speed_kmh")
weather_code = weather.get("weather_code")

col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        "🌡️ Temperatur",
        format_number(temperature, " °C"),
        border=True
    )

with col2:
    st.metric(
        "🌧️ Regenwahrscheinlichkeit",
        format_number(rain, " %"),
        border=True
    )

with col3:
    st.metric(
        "💨 Windgeschwindigkeit",
        format_number(wind, " km/h"),
        border=True
    )


# ==========================================
# KI-Wetter-Sentiment
# ==========================================

st.markdown("### 🧠 KI-Wetterbewertung")

# Statische CSS-Formatierung; dynamische Inhalte
# werden mit html.escape abgesichert.
st.html(
    f"""
    <div style="
        background-color: {score_background};
        border-left: 6px solid {score_color};
        border-radius: 14px;
        padding: 24px;
        margin-bottom: 16px;
        color: #1f2937;
    ">
        <div style="
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 16px;
        ">
            <div>
                <div style="
                    font-size: 15px;
                    font-weight: 600;
                    color: {score_color};
                ">
                    {escape(score_label)}
                </div>
                <div style="
                    font-size: 13px;
                    margin-top: 4px;
                ">
                    Wetter-Sentiment
                </div>
            </div>
            <div style="
                font-size: 38px;
                font-weight: 800;
                color: {score_color};
            ">
                {score}/10
            </div>
        </div>
        <p style="
            margin: 16px 0 0;
            line-height: 1.6;
            font-size: 16px;
        ">
            {escape(str(summary))}
        </p>
    </div>
    """
)


# ==========================================
# Outdoor-Aktivitäten
# ==========================================

st.markdown("### 🌳 Aktivitäten mit eurer Tochter")
st.caption(
    "Drei Vorschläge, die dein lokales "
    "Llama-3.2-Modell anhand der Wetterdaten erstellt hat."
)

if not activities:
    st.info("Noch keine Aktivitäten vorhanden.")

else:
    for activity in activities[:3]:
        name = activity.get("name", "Aktivität")
        description = activity.get("description", "")
        reason = activity.get("reason", "")

        # Bullet-Point mit Details
        st.markdown(
            f"- **{name}**"
        )
        if description:
            st.write(description)

        if reason:
            st.caption(f"💡 {reason}")


# ==========================================
# Zeitstempel und Rohdaten
# ==========================================

st.divider()

processed_at = metadata.get("processed_at")

if isinstance(processed_at, datetime):
    # MongoDB-Datumswerte sind normalerweise UTC.
    if processed_at.tzinfo is None:
        processed_at = processed_at.replace(tzinfo=timezone.utc)

    local_time = processed_at.astimezone().strftime(
        "%d.%m.%Y – %H:%M:%S %Z"
    )

    st.caption(f"🕒 Letzte KI-Verarbeitung: {local_time}")

if refresh:
    st.toast("Daten aus MongoDB neu geladen.", icon="✅")

with st.expander("🔍 Technische Details / Rohdaten"):
    st.write("**KI-Modell:**", metadata.get("model", "Unbekannt"))
    st.write("**Wettercode:**", weather_code)
    st.write("**Kafka-Topic:**", metadata.get("kafka_topic", "–"))
    st.write("**Kafka-Offset:**", metadata.get("kafka_offset", "–"))

    st.json({
        "weather": weather,
        "ai_analysis": analysis
    })
