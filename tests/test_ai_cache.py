from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from ai_cache import (
    get_cached_analysis,
    store_cached_analysis,
    weather_fingerprint,
)


def sample_weather():
    return {
        "location_id": 123,
        "city": "Lippstadt",
        "temperature_c": 13.8,
        "rain_probability_percent": 35,
        "wind_speed_kmh": 22.5,
        "weather_code": 3,
        "timestamp": "2026-10-24T15:00:00Z",
    }


def test_identical_weather_has_same_fingerprint():
    first = sample_weather()
    second = sample_weather()

    assert weather_fingerprint(first, "qwen3:8b") == weather_fingerprint(
        second, "qwen3:8b"
    )


def test_timestamp_does_not_change_fingerprint():
    first = sample_weather()
    second = sample_weather()
    second["timestamp"] = "2026-10-24T15:05:00Z"

    assert weather_fingerprint(first, "qwen3:8b") == weather_fingerprint(
        second, "qwen3:8b"
    )


def test_temperature_changes_fingerprint():
    first = sample_weather()
    second = sample_weather()
    second["temperature_c"] = 15.0

    assert weather_fingerprint(first, "qwen3:8b") != weather_fingerprint(
        second, "qwen3:8b"
    )


def test_model_changes_fingerprint():
    weather = sample_weather()

    assert weather_fingerprint(weather, "qwen3:8b") != weather_fingerprint(
        weather, "llama3.2"
    )


def test_expired_cache_returns_none():
    collection = MagicMock()
    collection.find_one.return_value = None

    now = datetime.now(timezone.utc)

    result = get_cached_analysis(
        collection,
        "fingerprint",
        now=now,
    )

    assert result is None

    query = collection.find_one.call_args.args[0]

    assert query["_id"] == "fingerprint"
    assert query["created_at"]["$gte"] == (now - timedelta(minutes=60))


def test_cache_hit_returns_analysis():
    collection = MagicMock()
    collection.find_one.return_value = {"analysis": {"sentiment_score": 8}}

    result = get_cached_analysis(
        collection,
        "fingerprint",
    )

    assert result["sentiment_score"] == 8


def test_store_cache_uses_upsert():
    collection = MagicMock()

    store_cached_analysis(
        collection,
        "fingerprint",
        "qwen3:8b",
        {"sentiment_score": 8},
    )

    assert collection.update_one.call_args.kwargs["upsert"] is True
