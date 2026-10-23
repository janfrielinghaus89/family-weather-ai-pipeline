"""Unit tests without requiring Kafka, MongoDB, Ollama or Streamlit."""

import pytest

from weather_utils import format_number, get_sentiment_style


@pytest.mark.parametrize(
    ("value", "suffix", "expected"),
    [
        (12.5, " °C", "12,5 °C"),
        (0, " %", "0,0 %"),
        (None, " °C", "–"),
        ("invalid", " °C", "–"),
    ],
)
def test_format_number(value, suffix, expected):
    assert format_number(value, suffix) == expected


@pytest.mark.parametrize(
    ("score", "expected_color"),
    [
        (1, "#991b1b"),
        (4, "#991b1b"),
        (5, "#854d0e"),
        (7, "#854d0e"),
        (8, "#166534"),
        (10, "#166534"),
    ],
)
def test_sentiment_colors(score, expected_color):
    foreground, background, label = get_sentiment_style(score)
    assert foreground == expected_color
    assert background.startswith("#")
    assert label
