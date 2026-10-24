import json

import pytest

from pipeline_resilience import (
    InvalidWeatherRecord,
    retry_operation,
    validate_weather,
)


def valid_weather():
    return {
        "city": "Lippstadt",
        "temperature_c": 13.8,
        "rain_probability_percent": 35,
        "wind_speed_kmh": 22.5,
        "weather_code": 3,
        "location_id": 12345,
    }


def test_valid_weather():
    data = validate_weather(json.dumps(valid_weather()).encode())
    assert data["city"] == "Lippstadt"
    assert data["location_id"] == 12345


def test_invalid_json():
    with pytest.raises(InvalidWeatherRecord):
        validate_weather(b"{invalid json")


def test_missing_temperature():
    weather = valid_weather()
    del weather["temperature_c"]

    with pytest.raises(InvalidWeatherRecord):
        validate_weather(json.dumps(weather).encode())


def test_invalid_rain_probability():
    weather = valid_weather()
    weather["rain_probability_percent"] = 150

    with pytest.raises(InvalidWeatherRecord):
        validate_weather(json.dumps(weather).encode())


def test_retry_succeeds():
    calls = []

    def operation():
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError("Ollama offline")
        return "success"

    result = retry_operation(
        operation,
        attempts=3,
        initial_delay=0,
        sleep=lambda _: None,
    )

    assert result == "success"
    assert len(calls) == 3


def test_retry_exhausted():
    def operation():
        raise ConnectionError("Ollama offline")

    with pytest.raises(ConnectionError):
        retry_operation(
            operation,
            attempts=3,
            initial_delay=0,
            sleep=lambda _: None,
        )
