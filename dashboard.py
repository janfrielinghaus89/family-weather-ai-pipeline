import os
from datetime import datetime, timedelta, timezone
from html import escape
from dotenv import load_dotenv
from urllib.parse import quote_plus
from weather_utils import format_number, get_sentiment_style

import pandas as pd
import plotly.graph_objects as go
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


st.set_page_config(page_title="Familienwetter", page_icon="🌤️", layout="wide")


# ==========================================
# MongoDB
# ==========================================


@st.cache_resource
def get_database():
    """MongoDB-Verbindung für Streamlit wiederverwenden."""
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    return client[DATABASE]


def get_available_cities() -> list[str]:
    """Ermittelt alle Städte mit gespeicherten Wetterdaten."""
    db = get_database()

    # Städte aus dem Feld "city" auslesen
    cities = db[COLLECTION].distinct("city")

    return sorted(city for city in cities if isinstance(city, str) and city.strip())


def load_latest_weather(city: str | None = None):
    """Lädt die neueste Wetteranalyse einer bestimmten Stadt."""
    db = get_database()

    query = {}

    if city is not None:
        query["city"] = city

    return db[COLLECTION].find_one(filter=query, sort=[("metadata.processed_at", -1)])


def load_weather_history(city: str | None, days: int = 7) -> pd.DataFrame:
    """Lädt historische Wetter- und Sentiment-Daten."""

    db = get_database()

    start_date = datetime.now(timezone.utc) - timedelta(days=days)

    query = {"metadata.processed_at": {"$gte": start_date}}

    if city is not None:
        query["city"] = city

    cursor = (
        db[COLLECTION]
        .find(
            query,
            {
                "_id": 0,
                "city": 1,
                "weather": 1,
                "ai_analysis.sentiment_score": 1,
                "metadata.processed_at": 1,
            },
        )
        .sort("metadata.processed_at", 1)
        .limit(10000)
    )

    records = []

    for document in cursor:
        weather = document.get("weather") or {}
        analysis = document.get("ai_analysis") or {}
        metadata = document.get("metadata") or {}

        timestamp = metadata.get("processed_at")

        if not isinstance(timestamp, datetime):
            continue

        records.append(
            {
                "timestamp": timestamp,
                "city": document.get("city", "Unbekannt"),
                "temperature": weather.get("temperature_c"),
                "rain_probability": weather.get("rain_probability_percent"),
                "wind_speed": weather.get("wind_speed_kmh"),
                "sentiment": analysis.get("sentiment_score"),
            }
        )

    columns = [
        "timestamp",
        "city",
        "temperature",
        "rain_probability",
        "wind_speed",
        "sentiment",
    ]

    df = pd.DataFrame(records, columns=columns)

    if df.empty:
        return df

    # UTC-Zeitstempel korrekt interpretieren
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(
        "Europe/Berlin"
    )

    numeric_columns = ["temperature", "rain_probability", "wind_speed", "sentiment"]

    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    return df.sort_values("timestamp")


# ==========================================
# Darstellung
# ==========================================


def create_history_chart(
    df: pd.DataFrame,
    column: str,
    title: str,
    color: str,
    unit: str,
    y_range: list | None = None,
) -> go.Figure:
    """Erstellt ein interaktives Zeitreihendiagramm."""

    fig = go.Figure()

    # Separate Kurve pro Stadt
    colors = [color, "#a855f7", "#14b8a6", "#eab308", "#ef4444", "#64748b"]

    for index, (city, city_data) in enumerate(df.groupby("city")):
        fig.add_trace(
            go.Scatter(
                x=city_data["timestamp"],
                y=city_data[column],
                mode="lines+markers",
                name=str(city),
                line={"color": colors[index % len(colors)], "width": 3},
                marker={"size": 5},
                connectgaps=False,
                hovertemplate=(
                    "%{x|%d.%m.%Y %H:%M}"
                    f"<br>{title}: %{{y:.1f}} {unit}"
                    "<extra>%{fullData.name}</extra>"
                ),
            )
        )

    fig.update_layout(
        title=title,
        height=320,
        margin={"l": 10, "r": 10, "t": 55, "b": 15},
        hovermode="x unified",
        xaxis_title=None,
        yaxis_title=unit,
        showlegend=df["city"].nunique() > 1,
        legend={"orientation": "h", "y": 1.15},
    )

    fig.update_xaxes(showgrid=True, rangeslider_visible=False)

    fig.update_yaxes(showgrid=True, range=y_range)

    return fig


# ==========================================
# Header
# ==========================================

st.title("🌤️ Familienwetter")
st.caption(
    "KI-gestützte Wetteranalyse mit Open-Meteo · Apache Kafka · Ollama · MongoDB"
)

header_left, header_right = st.columns([4, 1], vertical_alignment="center")

with header_left:
    st.subheader("Aktuelle Wetterübersicht")

with header_right:
    refresh = st.button(
        "🔄 Daten aktualisieren", use_container_width=True, type="primary"
    )


# ==========================================
# Stadtfilter und Datenabruf
# ==========================================

try:
    available_cities = get_available_cities()

    if not available_cities:
        st.warning(
            "Noch keine Städte in MongoDB vorhanden. "
            "Bitte Producer und Consumer starten."
        )
        st.stop()

    # Auswahlmöglichkeiten dynamisch erstellen
    city_options = ["Alle Städte"] + available_cities

    selected_city = st.selectbox(
        "📍 Stadt auswählen",
        options=city_options,
        index=0,
        key="selected_city",
        help="Zeigt alle Städte an, für die Wetterdaten gespeichert wurden.",
    )

    city_filter = None if selected_city == "Alle Städte" else selected_city

    # Neueste Wetteranalyse für den ausgewählten Filter laden
    document = load_latest_weather(city_filter)

except PyMongoError as error:
    st.error(f"MongoDB ist nicht erreichbar: {error}")
    st.stop()


if not document:
    st.warning(
        "Noch keine Wetteranalysen vorhanden. Bitte Producer und Consumer starten."
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

score_color, score_background, score_label = get_sentiment_style(score)


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
    st.metric("🌡️ Temperatur", format_number(temperature, " °C"), border=True)

with col2:
    st.metric("🌧️ Regenwahrscheinlichkeit", format_number(rain, " %"), border=True)

with col3:
    st.metric("💨 Windgeschwindigkeit", format_number(wind, " km/h"), border=True)


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
        st.markdown(f"- **{name}**")
        if description:
            st.write(description)

        if reason:
            st.caption(f"💡 {reason}")


# ==========================================
# Historische Wetterdaten
# ==========================================

st.divider()
st.header("📈 Wetterhistorie")

# Filter übernehmen
history_city = None if selected_city == "Alle Städte" else selected_city

filter_col, info_col = st.columns([2, 3])

with filter_col:
    history_period = st.selectbox(
        "Zeitraum auswählen",
        options=[1, 7, 30, 90],
        format_func=lambda days: (
            "Letzte 24 Stunden" if days == 1 else f"Letzte {days} Tage"
        ),
        index=1,
    )

with info_col:
    st.caption("Die Diagramme verwenden gespeicherte Wetteranalysen aus MongoDB.")

try:
    history_df = load_weather_history(city=history_city, days=history_period)

except PyMongoError as error:
    st.error(f"Fehler beim Laden der Historie: {error}")
    history_df = pd.DataFrame()


if history_df.empty:
    st.info("Für diesen Zeitraum sind noch keine historischen Wetterdaten vorhanden.")

else:
    # Kennzahlen
    metric_1, metric_2, metric_3 = st.columns(3)

    with metric_1:
        average_temp = history_df["temperature"].mean()
        st.metric(
            "Ø Temperatur", f"{average_temp:.1f} °C" if pd.notna(average_temp) else "–"
        )

    with metric_2:
        average_sentiment = history_df["sentiment"].mean()
        st.metric(
            "Ø Wetter-Sentiment",
            f"{average_sentiment:.1f}/10" if pd.notna(average_sentiment) else "–",
        )

    with metric_3:
        st.metric("Gespeicherte Analysen", len(history_df))

    # Temperatur
    fig_temp = create_history_chart(
        history_df,
        column="temperature",
        title="Temperaturverlauf",
        color="#f97316",
        unit="°C",
    )

    st.plotly_chart(fig_temp, width="stretch", key="history_temperature")

    # Regenwahrscheinlichkeit
    fig_rain = create_history_chart(
        history_df,
        column="rain_probability",
        title="Regenwahrscheinlichkeit",
        color="#3b82f6",
        unit="%",
        y_range=[0, 100],
    )

    st.plotly_chart(fig_rain, width="stretch", key="history_rain")

    # KI-Sentiment
    fig_sentiment = create_history_chart(
        history_df,
        column="sentiment",
        title="KI-Wetter-Sentiment",
        color="#22c55e",
        unit="Score",
        y_range=[1, 10],
    )

    st.plotly_chart(fig_sentiment, width="stretch", key="history_sentiment")

    # Rohdaten optional anzeigen
    with st.expander("📋 Historische Datensätze"):
        st.dataframe(
            history_df.sort_values("timestamp", ascending=False),
            width="stretch",
            hide_index=True,
        )

# ==========================================
# Zeitstempel und Rohdaten
# ==========================================

st.divider()

processed_at = metadata.get("processed_at")

if isinstance(processed_at, datetime):
    # MongoDB-Datumswerte sind normalerweise UTC.
    if processed_at.tzinfo is None:
        processed_at = processed_at.replace(tzinfo=timezone.utc)

    local_time = processed_at.astimezone().strftime("%d.%m.%Y – %H:%M:%S %Z")

    st.caption(f"🕒 Letzte KI-Verarbeitung: {local_time}")

if refresh:
    st.toast("Daten aus MongoDB neu geladen.", icon="✅")

with st.expander("🔍 Technische Details / Rohdaten"):
    st.write("**KI-Modell:**", metadata.get("model", "Unbekannt"))
    st.write("**Wettercode:**", weather_code)
    st.write("**Kafka-Topic:**", metadata.get("kafka_topic", "–"))
    st.write("**Kafka-Offset:**", metadata.get("kafka_offset", "–"))

    st.json({"weather": weather, "ai_analysis": analysis})
