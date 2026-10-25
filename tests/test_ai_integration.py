from unittest.mock import MagicMock

from pydantic import BaseModel

from ai.base import AIProvider


class ExampleWeatherAnalysis(BaseModel):
    sentiment_score: int


def test_provider_can_return_structured_analysis():
    provider = MagicMock(spec=AIProvider)

    provider.generate.return_value = ExampleWeatherAnalysis(sentiment_score=8)

    result = provider.generate(
        system_prompt="Weather expert",
        user_prompt="Analyze today's weather",
        response_model=ExampleWeatherAnalysis,
    )

    assert result.sentiment_score == 8
    provider.generate.assert_called_once()


def test_different_providers_get_different_cache_ids():
    first = "ollama:qwen3:8b"
    second = "openai:qwen3:8b"

    assert first != second
