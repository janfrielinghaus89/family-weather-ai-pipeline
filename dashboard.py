import os
from datetime import datetime, timedelta, timezone
from html import escape
from dotenv import load_dotenv
from urllib.parse import quote_plus
from weather_utils import format_number, get_sentiment_style
import requests

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pydantic import ValidationError
from family import (
    Child,
    FamilyProfile,
    FamilyAdvice,
    generate_advice,
    get_cached_advice,
    load_profile,
    recommendation_key,
    save_advice,
    save_profile,
)

from location_service import (
    search_locations,
    location_label,
    add_tracked_location,
    list_tracked_locations,
    remove_tracked_location,
)

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


# ==========================================
# Standorte suchen und verwalten
# ==========================================

with st.sidebar.expander("📍 Standorte verwalten"):
    search_text = st.text_input(
        "Stadt oder Postleitzahl", placeholder="Lippstadt oder 59555"
    )

    country_code = st.selectbox("Land", ["DE", "AT", "CH", "Alle"])

    if st.button("🔍 Suchen"):
        try:
            matches = search_locations(
                search_text, None if country_code == "Alle" else country_code
            )

            st.session_state["location_matches"] = matches

        except requests.RequestException as error:
            st.error(f"Suche fehlgeschlagen: {error}")

    matches = st.session_state.get("location_matches", [])

    if matches:
        selected_index = st.selectbox(
            "Gefundene Standorte",
            options=list(range(len(matches))),
            format_func=lambda i: location_label(matches[i]),
        )

        if st.button("➕ Standort hinzufügen"):
            added = add_tracked_location(get_database(), matches[selected_index])

            if added:
                st.success("Standort hinzugefügt!")
            else:
                st.info("Standort bereits vorhanden.")

            st.rerun()

    st.divider()
    st.markdown("**Gespeicherte Standorte**")

    tracked = list_tracked_locations(get_database())

    for location in tracked:
        col_name, col_remove = st.columns([4, 1])

        with col_name:
            st.caption(location_label(location))

        with col_remove:
            if st.button("✕", key=f"remove_{location['_id']}"):
                remove_tracked_location(get_database(), location["_id"])
                st.rerun()


# ==========================================
# Stadtauswahl aus tracked_locations
# ==========================================

tracked_locations = list_tracked_locations(get_database())

location_options = {str(loc["_id"]): loc for loc in tracked_locations}


def location_query(location: dict | None) -> dict:
    """Erstellt einen Filter für einen eindeutigen Standort."""
    if location is None:
        return {}

    return {"weather.location_id": location["_id"]}


def load_latest_weather(location: dict | None = None):
    db = get_database()

    return db[COLLECTION].find_one(
        filter=location_query(location), sort=[("metadata.processed_at", -1)]
    )


def load_weather_history(location: dict | None, days: int = 7) -> pd.DataFrame:
    """Lädt historische Wetter- und Sentiment-Daten."""

    db = get_database()

    start_date = datetime.now(timezone.utc) - timedelta(days=days)

    query = {"metadata.processed_at": {"$gte": start_date}}

    query.update(location_query(location))

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
# Standortauswahl aus MongoDB
# ==========================================

try:
    tracked_locations = list_tracked_locations(get_database())

    location_options = {
        str(location["_id"]): location for location in tracked_locations
    }

    selected_location_id = st.selectbox(
        "📍 Stadt auswählen",
        options=[None] + list(location_options.keys()),
        format_func=lambda location_id: (
            "Alle Standorte"
            if location_id is None
            else location_label(location_options[location_id])
        ),
        key="selected_location_id",
    )

    selected_location = (
        location_options.get(selected_location_id)
        if selected_location_id is not None
        else None
    )

    document = load_latest_weather(selected_location)

except PyMongoError as error:
    st.error(f"MongoDB ist nicht erreichbar: {error}")
    st.stop()

if document is None:
    st.info(
        "Für den ausgewählten Standort liegen noch keine verarbeiteten Wetterdaten vor."
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
# Familienprofile & personalisierte KI
# ==========================================

st.divider()
st.header("👨‍👩‍👧 Familienprofil & Kleidung")

db = get_database()

try:
    profile = load_profile(db)
except (ValidationError, PyMongoError) as error:
    st.error(f"Familienprofil konnte nicht geladen werden: {error}")
    st.stop()

ACTIVITY_OPTIONS = [
    "Spaziergänge",
    "Spielplätze",
    "Tiere",
    "Natur",
    "Radfahren",
    "Wandern",
    "Wald",
    "Parks",
    "Wasser",
    "Garten",
]

# Anzahl außerhalb des Formulars, damit zusätzliche
# Kinderfelder direkt erscheinen können.
child_count = st.number_input(
    "Anzahl der Kinder", min_value=1, max_value=4, value=len(profile.children), step=1
)

with st.form("family_profile_form"):
    children = []

    for index in range(child_count):
        st.subheader(f"Kind {index + 1}")

        existing = (
            profile.children[index]
            if index < len(profile.children)
            else Child(age_months=12)
        )

        age = st.number_input(
            "Alter in Monaten",
            min_value=0,
            max_value=216,
            value=existing.age_months,
            step=1,
            key=f"child_age_{index}",
        )

        gender_options = ["Keine Angabe", "Weiblich", "Männlich", "Divers"]

        gender = st.selectbox(
            "Geschlecht (optional)",
            options=gender_options,
            index=(
                gender_options.index(existing.gender)
                if existing.gender in gender_options
                else 0
            ),
            key=f"child_gender_{index}",
        )

        children.append(Child(age_months=age, gender=gender))

    interests = st.multiselect(
        "Bevorzugte Aktivitäten",
        options=ACTIVITY_OPTIONS,
        default=[item for item in profile.interests if item in ACTIVITY_OPTIONS],
    )

    submitted = st.form_submit_button("💾 Familienprofil speichern", type="primary")

if submitted:
    new_profile = FamilyProfile(children=children, interests=interests)

    try:
        save_profile(db, new_profile)
    except PyMongoError as error:
        st.error(f"Speichern fehlgeschlagen: {error}")
    else:
        st.success("Familienprofil gespeichert.")
        st.rerun()


# ==========================================
# Personalisierte KI-Auswertung
# ==========================================

st.subheader("🧥 Individuelle Empfehlungen")

st.caption(
    "Die Empfehlungen berücksichtigen das gespeicherte "
    "Familienprofil und die aktuell ausgewählten Wetterdaten."
)

cache_key = recommendation_key(document["_id"], profile)

try:
    cached = get_cached_advice(db, cache_key)
except PyMongoError as error:
    st.error(f"Empfehlungen konnten nicht geladen werden: {error}")
    cached = None

if st.button("✨ Empfehlungen generieren", type="primary"):
    if cached is None:
        with st.spinner("Ollama analysiert die Wetterlage ..."):
            try:
                advice = generate_advice(weather, profile)

                save_advice(db, cache_key, document["_id"], profile, advice)

                st.success("Empfehlungen gespeichert.")
                st.rerun()

            except Exception as error:
                st.error(f"KI-Auswertung fehlgeschlagen: {error}")
    else:
        st.info("Für dieses Wetter und Familienprofil existieren bereits Empfehlungen.")


# ==========================================
# Ergebnisse anzeigen
# ==========================================

if cached is not None:
    advice = FamilyAdvice.model_validate(cached["advice"])

    st.info(advice.summary)

    st.markdown("#### Kleidungsempfehlungen")

    for child_advice in sorted(advice.clothing, key=lambda item: item.child_number):
        with st.container(border=True):
            st.markdown(f"**👶 Kind {child_advice.child_number}**")

            for clothing_item in child_advice.clothing:
                st.markdown(f"- {clothing_item}")

            st.caption(child_advice.explanation)

    st.markdown("#### 🌳 Personalisierte Aktivitäten")

    for activity in advice.activities:
        st.markdown(f"- {activity}")

else:
    st.info("Speichere dein Familienprofil und klicke auf 'Empfehlungen generieren'.")


# ==========================================
# Historische Wetterdaten
# ==========================================

st.divider()
st.header("📈 Wetterhistorie")

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
    history_df = load_weather_history(location=selected_location, days=history_period)

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
