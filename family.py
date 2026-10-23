import hashlib
import json
import os
from datetime import datetime, timezone

from config import OLLAMA_HOST, OLLAMA_MODEL
from ollama import Client
from pydantic import BaseModel, Field, ValidationError


PROFILE_ID = "local-family"
RECOMMENDATION_VERSION = "3"


class Child(BaseModel):
    age_months: int = Field(ge=0, le=216)
    gender: str = "Keine Angabe"


class FamilyProfile(BaseModel):
    children: list[Child] = Field(min_length=1, max_length=4)
    interests: list[str] = Field(default_factory=list)


class ClothingAdvice(BaseModel):
    child_number: int
    clothing: list[str] = Field(min_length=2, max_length=6)
    explanation: str


class FamilyAdvice(BaseModel):
    summary: str
    clothing: list[ClothingAdvice]
    activities: list[str] = Field(min_length=3, max_length=3)


def load_profile(db) -> FamilyProfile:
    """Lädt das lokale Familienprofil."""
    document = db.family_profiles.find_one({"_id": PROFILE_ID})

    if document is None:
        return FamilyProfile(children=[Child(age_months=12)], interests=[])

    return FamilyProfile.model_validate(
        {
            "children": document.get("children", []),
            "interests": document.get("interests", []),
        }
    )


def save_profile(db, profile: FamilyProfile) -> None:
    """Speichert das Familienprofil."""
    db.family_profiles.update_one(
        {"_id": PROFILE_ID},
        {"$set": {**profile.model_dump(), "updated_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


def recommendation_key(weather_id, profile: FamilyProfile) -> str:
    """Erzeugt einen stabilen Cache-Schlüssel."""
    payload = {
        "weather_id": str(weather_id),
        "profile": profile.model_dump(),
        "model": OLLAMA_MODEL,
        "version": RECOMMENDATION_VERSION,
    }

    content = json.dumps(payload, sort_keys=True, ensure_ascii=False)

    return hashlib.sha256(content.encode()).hexdigest()


def get_cached_advice(db, key: str) -> dict | None:
    return db.family_recommendations.find_one({"_id": key})



def get_weather_context(weather: dict) -> str:
    """Erstellt eine wetterabhängige Einordnung für Ollama."""

    temp = float(weather["temperature_c"])
    wind = float(weather["wind_speed_kmh"])
    rain = float(weather["rain_probability_percent"])

    if temp < 5:
        temperature_category = "sehr kalt"
    elif temp < 12:
        temperature_category = "kalt"
    elif temp < 18:
        temperature_category = "kühl"
    elif temp < 25:
        temperature_category = "mild bis warm"
    else:
        temperature_category = "heiß"

    wind_category = (
        "spürbarer Wind" if wind >= 20
        else "wenig bis mäßiger Wind"
    )

    rain_category = (
        "hohe Regenwahrscheinlichkeit" if rain >= 70
        else "mittlere Regenwahrscheinlichkeit" if rain >= 40
        else "geringe Regenwahrscheinlichkeit"
    )

    return (
        f"Temperatur: {temp} °C ({temperature_category}). "
        f"Wind: {wind} km/h ({wind_category}). "
        f"Regenwahrscheinlichkeit: {rain}% ({rain_category}). "
        "Die Einordnung dient nur als Orientierung; "
        "Wind, Sonne und Aufenthaltsdauer beeinflussen "
        "das Wärmeempfinden zusätzlich."
    )


def get_child_context(age_months: int) -> str:
    """Gibt eine altersgerechte Aktivitätseinordnung zurück."""

    if age_months < 6:
        return (
            "Junger Säugling: kurze, geschützte Aufenthalte, "
            "Tragen oder geeigneter Kinderwagen. "
            "Kein eigenständiges Spielen voraussetzen."
        )
    elif age_months < 12:
        return (
            "Älterer Säugling: möglicherweise noch nicht mobil, "
            "sitzt oder liegt häufig im Kinderwagen. "
            "Beobachten, Entdecken und kurze Ausflüge bevorzugen."
        )
    elif age_months < 24:
        return (
            "Kleinkind: möglicherweise erste Gehversuche, "
            "kurze Spiel- und Bewegungseinheiten."
        )
    elif age_months < 48:
        return (
            "Kleinkind: aktive Bewegung und einfache "
            "Entdeckungsspiele möglich."
        )
    else:
        return (
            "Älteres Kind: aktive Bewegung, altersgerechte "
            "Spiele und längere Ausflüge möglich."
        )


def generate_advice(weather: dict, profile: FamilyProfile) -> FamilyAdvice:
    """Erzeugt personalisierte Empfehlungen mit Ollama."""

    client = Client(host=OLLAMA_HOST, timeout=120, trust_env=False)

    child_count = len(profile.children)
    child_numbers = list(range(1, child_count + 1))

    
    child_count = len(profile.children)
    child_numbers = list(range(1, child_count + 1))

    child_contexts = "\n".join(
        f"Kind {i}: {child.age_months} Monate. "
        f"{get_child_context(child.age_months)}"
        for i, child in enumerate(profile.children, start=1)
    )

    prompt = f"""
Du bist ein erfahrener Familien- und Wetterassistent.
Deine Aufgabe ist es, für die AKTUELLEN Wetterbedingungen
konkrete, altersgerechte Empfehlungen zu formulieren.

AKTUELLE WETTERDATEN:
{json.dumps(weather, ensure_ascii=False, indent=2)}

WETTEREINORDNUNG:
{get_weather_context(weather)}

FAMILIE:
{profile.model_dump_json(indent=2)}

ENTWICKLUNGSSTAND DER KINDER:
{child_contexts}

Die Familie hat GENAU {child_count} Kinder.
Gültige Kindernummern: {child_numbers}.

AUFGABE 1 - KLEIDUNG:
Erstelle genau eine Kleidungsempfehlung pro Kind.

- Empfiehl eine ZUSAMMENPASSENDE Kombination von Kleidungsstücken.
- Beziehe dich ausschließlich auf die aktuelle Wetterlage.
- Keine Empfehlungen für andere Jahreszeiten oder Temperaturen.
- Berücksichtige Wind und Regenwahrscheinlichkeit.
- Unterscheide sitzende Babys von aktiv laufenden Kindern.
- Nenne 3 bis 6 konkrete Kleidungsstücke oder Schichten.
- Erkläre, warum diese Kombination sinnvoll ist.
- Keine widersprüchlichen Empfehlungen.
- Bei Säuglingen mögliche Überwärmung berücksichtigen.
- Keine pauschalen Aussagen wie "für kalte Tage" oder
  "für warme Tage".

AUFGABE 2 - AKTIVITÄTEN:
Erstelle GENAU drei möglichst unterschiedliche
Outdoor-Aktivitäten für die gesamte Familie.

- Passe die Aktivitäten an das jüngste Kind an.
- Berücksichtige die Interessen der Familie:
  {", ".join(profile.interests) or "Keine angegeben"}
- Wähle drei unterschiedliche Aktivitätstypen.
- Vermeide doppelte Vorschläge mit anderen Namen.
- Beschreibe bei jeder Aktivität konkret, was
  Eltern und Kinder dabei machen können.
- Nenne eine angemessene ungefähre Dauer.
- Berücksichtige die aktuellen Wetterbedingungen.
- Erfinde keine lokalen Veranstaltungen oder Einrichtungen.
- Empfiehl bei schlechtem Wetter kürzere und
  geschütztere Aufenthalte, falls vertretbar.

QUALITÄTSREGELN:
- Deutsch, konkret, alltagstauglich.
- Keine allgemeinen Ratgebertexte.
- Keine erfundenen Wetterdaten.
- Keine Aktivitäten, die dem Entwicklungsstand widersprechen.
- Keine pauschalen Sicherheitsversprechen.
- Genau {child_count} Kleidungsempfehlungen.
- Genau 3 unterschiedliche Aktivitäten.
- Gib ausschließlich gültiges JSON gemäß Schema zurück.
"""


    expected = list(range(1, len(profile.children) + 1))

    for attempt in range(1, 4):
        response = client.chat(
            model=OLLAMA_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Du bist ein Familienassistent. "
                        "Halte dich exakt an die Anzahl "
                        "und Nummerierung der Kinder."
                    )
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            format=FamilyAdvice.model_json_schema(),
            options={"temperature": 0},
            stream=False
        )

        try:
            advice = FamilyAdvice.model_validate_json(
                response.message.content
            )

            actual = sorted(
                item.child_number
                for item in advice.clothing
            )

            if actual != expected:
                raise ValueError(
                    f"Erwartete Kindernummern: {expected}, "
                    f"erhalten: {actual}"
                )

            return advice

        except (ValidationError, ValueError) as error:
            if attempt == 3:
                raise ValueError(
                    "Ollama hat nach drei Versuchen "
                    "keine gültige Familienempfehlung erstellt."
                ) from error

            print(
                f"Ungültige KI-Antwort "
                f"(Versuch {attempt}/3): {error}"
            )


def save_advice(
    db, key: str, weather_id, profile: FamilyProfile, advice: FamilyAdvice
) -> None:
    """Speichert eine generierte Empfehlung."""
    db.family_recommendations.update_one(
        {"_id": key},
        {
            "$setOnInsert": {
                "weather_id": str(weather_id),
                "profile": profile.model_dump(),
                "model": OLLAMA_MODEL,
                "recommendation_version": RECOMMENDATION_VERSION,
                "advice": advice.model_dump(),
                "created_at": datetime.now(timezone.utc),
            }
        },
        upsert=True,
    )
