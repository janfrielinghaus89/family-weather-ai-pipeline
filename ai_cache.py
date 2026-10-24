import hashlib
import json
from datetime import datetime, timedelta, timezone

CACHE_MAX_AGE_MINUTES = 60
PROMPT_VERSION = "1"

WEATHER_FIELDS = (
    "temperature_c",
    "rain_probability_percent",
    "wind_speed_kmh",
    "weather_code",
)


def weather_fingerprint(weather: dict, model: str) -> str:
    """Erzeugt einen stabilen Schlüssel für eine Wetterlage."""

    location_id = weather.get("location_id")

    # Legacy-Daten ohne location_id unterstützen
    if location_id is not None:
        location = {"location_id": str(location_id)}
    else:
        location = {
            "city": str(weather["city"]).strip().casefold(),
            "country": str(weather.get("country", "")).strip().casefold(),
            "latitude": weather.get("latitude"),
            "longitude": weather.get("longitude"),
        }

    payload = {
        "location": location,
        "weather": {field: weather[field] for field in WEATHER_FIELDS},
        "model": model,
        "prompt_version": PROMPT_VERSION,
    }

    canonical = json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )

    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def get_cached_analysis(
    collection,
    fingerprint: str,
    now: datetime | None = None,
) -> dict | None:
    """Lädt ein ausreichend frisches KI-Ergebnis."""

    now = now or datetime.now(timezone.utc)

    cutoff = now - timedelta(minutes=CACHE_MAX_AGE_MINUTES)

    entry = collection.find_one(
        {
            "_id": fingerprint,
            "created_at": {"$gte": cutoff},
        }
    )

    if entry is None:
        return None

    return entry.get("analysis")


def store_cached_analysis(
    collection,
    fingerprint: str,
    model: str,
    analysis: dict,
    now: datetime | None = None,
) -> None:
    """Speichert oder erneuert einen KI-Cache-Eintrag."""

    now = now or datetime.now(timezone.utc)

    collection.update_one(
        {"_id": fingerprint},
        {
            "$set": {
                "model": model,
                "prompt_version": PROMPT_VERSION,
                "analysis": analysis,
                "created_at": now,
            }
        },
        upsert=True,
    )
