from dataclasses import dataclass

from ai.base import AIProvider
from ai.factory import create_ai_provider
from config import (
    AI_MODEL,
    AI_PROVIDER,
    CUSTOM_AI_API_KEY,
    CUSTOM_AI_BASE_URL,
    OLLAMA_HOST,
    OLLAMA_MODEL,
    OPENAI_API_KEY,
)


@dataclass(frozen=True)
class AISettings:
    provider: str
    model: str
    ollama_host: str
    api_key: str | None = None
    base_url: str | None = None


def get_ai_settings() -> AISettings:
    """Lädt die aktuelle AI-Konfiguration aus der Umgebung."""

    provider = AI_PROVIDER

    if provider == "ollama":
        return AISettings(
            provider="ollama",
            model=AI_MODEL or OLLAMA_MODEL,
            ollama_host=OLLAMA_HOST,
        )

    if not AI_MODEL:
        raise ValueError("AI_MODEL must be configured for external AI")

    if provider == "openai":
        return AISettings(
            provider="openai",
            model=AI_MODEL,
            ollama_host=OLLAMA_HOST,
            api_key=OPENAI_API_KEY,
        )

    if provider == "custom":
        return AISettings(
            provider="custom",
            model=AI_MODEL,
            ollama_host=OLLAMA_HOST,
            api_key=CUSTOM_AI_API_KEY,
            base_url=CUSTOM_AI_BASE_URL,
        )

    raise ValueError(f"Unsupported AI provider: {provider}")


def get_configured_provider() -> AIProvider:
    """Erstellt den Provider aus der aktuellen Konfiguration."""

    settings = get_ai_settings()

    return create_ai_provider(
        provider_name=settings.provider,
        model=settings.model,
        ollama_host=settings.ollama_host,
        api_key=settings.api_key,
        base_url=settings.base_url,
    )


def get_default_ai_settings() -> AISettings:
    """Liefert die lokale Standardkonfiguration."""

    return AISettings(
        provider="ollama",
        model=OLLAMA_MODEL,
        ollama_host=OLLAMA_HOST,
    )
