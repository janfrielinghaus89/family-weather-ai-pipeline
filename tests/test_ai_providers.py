from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel

from ai.factory import create_ai_provider
from ai.ollama_provider import OllamaProvider


class ExampleResponse(BaseModel):
    score: int


def test_factory_creates_ollama_provider():
    provider = create_ai_provider(
        provider_name="ollama",
        model="qwen3:8b",
        ollama_host="http://127.0.0.1:11434",
    )

    assert isinstance(provider, OllamaProvider)
    assert provider.model_name == "qwen3:8b"
    assert provider.provider_name == "ollama"


def test_factory_rejects_unknown_provider():
    with pytest.raises(ValueError, match="Unsupported"):
        create_ai_provider(
            provider_name="unknown",
            model="test",
            ollama_host="http://127.0.0.1:11434",
        )


@patch("ai.ollama_provider.ollama.Client")
def test_ollama_structured_response(mock_client_class):
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    mock_client.chat.return_value = {"message": {"content": '{"score": 8}'}}

    provider = OllamaProvider(
        host="http://127.0.0.1:11434",
        model="qwen3:8b",
    )

    result = provider.generate(
        system_prompt="System prompt",
        user_prompt="User prompt",
        response_model=ExampleResponse,
    )

    assert result.score == 8

    mock_client.chat.assert_called_once()
