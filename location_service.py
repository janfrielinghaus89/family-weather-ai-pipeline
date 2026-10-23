"""Geocoding and persistence of tracked Open-Meteo locations."""

from datetime import datetime, timezone

import requests

GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"


def search_locations(query: str, country_code: str | None = None) -> list[dict]:
    """Search cities or postal codes; return normalized Open-Meteo locations."""
    query = query.strip()
    if len(query) < 2:
        return []

    params = {"name": query, "count": 10, "language": "de"}
    if country_code:
        params["countryCode"] = country_code.upper()

    response = requests.get(GEOCODING_URL, params=params, timeout=12)
    response.raise_for_status()
    locations = []
    for item in response.json().get("results", []):
        if not all(k in item for k in ("id", "name", "latitude", "longitude")):
            continue
        locations.append(
            {
                "_id": int(item["id"]),
                "name": item["name"],
                "admin1": item.get("admin1", ""),
                "country": item.get("country", ""),
                "country_code": item.get("country_code", ""),
                "latitude": float(item["latitude"]),
                "longitude": float(item["longitude"]),
                "timezone": item.get("timezone", "UTC"),
            }
        )
    return locations


def location_label(location: dict) -> str:
    return ", ".join(
        str(value)
        for value in (location["name"], location.get("admin1"), location.get("country"))
        if value
    )


def add_tracked_location(db, location: dict) -> bool:
    """Upsert location by immutable Open-Meteo ID; return True if inserted."""
    payload = {key: value for key, value in location.items() if key != "_id"}
    result = db.tracked_locations.update_one(
        {"_id": int(location["_id"])},
        {"$set": payload, "$setOnInsert": {"created_at": datetime.now(timezone.utc)}},
        upsert=True,
    )
    return result.upserted_id is not None


def list_tracked_locations(db) -> list[dict]:
    return list(db.tracked_locations.find().sort("name", 1))


def remove_tracked_location(db, location_id: int) -> bool:
    return db.tracked_locations.delete_one({"_id": int(location_id)}).deleted_count == 1
