from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel

from ai.factory import create_ai_provider
from ai.openai_provider import OpenAIProvider


class ExampleResponse(BaseModel):
    score: int


def test_factory_creates_openai_provider():
    provider = create_ai_provider(
        provider_name="openai",
        model="test-model",
        ollama_host="http://localhost:11434",
        api_key="test-key",
    )

    assert isinstance(provider, OpenAIProvider)
    assert provider.provider_name == "openai"
    assert provider.model_name == "test-model"


def test_factory_creates_custom_provider():
    provider = create_ai_provider(
        provider_name="custom",
        model="custom-model",
        ollama_host="http://localhost:11434",
        api_key="test-key",
        base_url="https://example.com/v1",
    )

    assert provider.provider_name == "custom"


def test_missing_api_key_is_rejected():
    with pytest.raises(ValueError, match="API key"):
        OpenAIProvider(
            model="test-model",
            api_key="",
        )


@patch("ai.openai_provider.OpenAI")
def test_openai_structured_response(mock_openai):
    client = MagicMock()
    mock_openai.return_value = client

    message = MagicMock()
    message.refusal = None
    message.parsed = ExampleResponse(score=8)

    client.chat.completions.parse.return_value.choices = [MagicMock(message=message)]

    provider = OpenAIProvider(
        model="test-model",
        api_key="test-key",
    )

    result = provider.generate(
        system_prompt="You are a weather assistant.",
        user_prompt="Analyze the weather.",
        response_model=ExampleResponse,
    )

    assert result.score == 8
    client.chat.completions.parse.assert_called_once()


@patch("ai.openai_provider.OpenAI")
def test_custom_api_json_response(mock_openai):
    client = MagicMock()
    mock_openai.return_value = client

    message = MagicMock()
    message.content = '{"score": 7}'

    client.chat.completions.create.return_value.choices = [MagicMock(message=message)]

    provider = OpenAIProvider(
        model="custom-model",
        api_key="test-key",
        base_url="https://example.com/v1",
        custom=True,
    )

    result = provider.generate(
        system_prompt="System",
        user_prompt="User",
        response_model=ExampleResponse,
    )

    assert result.score == 7


@patch("ai.openai_provider.OpenAI")
def test_openai_refusal(mock_openai):
    client = MagicMock()
    mock_openai.return_value = client

    message = MagicMock()
    message.refusal = "Request refused"
    message.parsed = None

    client.chat.completions.parse.return_value.choices = [MagicMock(message=message)]

    provider = OpenAIProvider(
        model="test-model",
        api_key="test-key",
    )

    with pytest.raises(ValueError, match="refused"):
        provider.generate(
            system_prompt="System",
            user_prompt="User",
            response_model=ExampleResponse,
        )
