from unittest.mock import Mock, patch

from location_service import location_label, search_locations


def test_short_query_skips_request():
    with patch("location_service.requests.get") as get:
        assert search_locations("a") == []
        get.assert_not_called()


def test_geocoding_normalizes_results():
    response = Mock()
    response.json.return_value = {
        "results": [
            {
                "id": 123,
                "name": "Teststadt",
                "latitude": 51.2,
                "longitude": 8.1,
                "country": "Deutschland",
            }
        ]
    }
    with patch("location_service.requests.get", return_value=response) as get:
        result = search_locations("59555", "DE")
    assert result[0]["_id"] == 123
    assert result[0]["latitude"] == 51.2
    assert get.call_args.kwargs["params"]["countryCode"] == "DE"


def test_location_label():
    assert (
        location_label(
            {
                "name": "Lippstadt",
                "admin1": "Nordrhein-Westfalen",
                "country": "Deutschland",
            }
        )
        == "Lippstadt, Nordrhein-Westfalen, Deutschland"
    )
