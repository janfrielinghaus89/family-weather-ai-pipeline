import hashlib
from dataclasses import replace
from datetime import datetime, timezone
from urllib.parse import urlsplit

from ai.base import AIProvider
from ai.configuration import (
    AISettings,
    get_ai_settings,
    get_default_ai_settings,
)
from ai.factory import create_ai_provider

SETTINGS_ID = "active-ai"
SUPPORTED_PROVIDERS = ("ollama", "openai", "custom")


def load_ai_settings(db) -> AISettings:
    """Lädt die gespeicherte oder die Standardkonfiguration."""

    defaults = get_default_ai_settings()
    document = db.app_settings.find_one({"_id": SETTINGS_ID})

    if document is None:
        return get_ai_settings()

    provider = document["provider"]
    model = document["model"]
    base_url = document.get("base_url")

    # API-Keys werden niemals aus MongoDB geladen.
    from config import CUSTOM_AI_API_KEY, OPENAI_API_KEY

    api_key = {
        "ollama": None,
        "openai": OPENAI_API_KEY,
        "custom": CUSTOM_AI_API_KEY,
    }.get(provider)

    return AISettings(
        provider=provider,
        model=model,
        ollama_host=document.get("ollama_host", defaults.ollama_host),
        api_key=api_key,
        base_url=base_url,
    )


def validate_ai_settings(settings: AISettings) -> None:
    """Validiert die Konfiguration vor dem Speichern."""

    if settings.provider not in SUPPORTED_PROVIDERS:
        raise ValueError("Unbekannter KI-Anbieter")

    if not settings.model.strip():
        raise ValueError("Ein Modell muss angegeben werden")

    if settings.provider == "ollama":
        if not settings.ollama_host.strip():
            raise ValueError("Ollama-Host fehlt")

    if settings.provider == "custom":
        if not settings.base_url:
            raise ValueError("Custom API Base URL fehlt")

        parsed = urlsplit(settings.base_url)

        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError("Custom API benötigt eine HTTPS-URL")


def save_ai_settings(db, settings: AISettings) -> None:
    """Speichert ausschließlich nicht geheime Einstellungen."""

    validate_ai_settings(settings)

    db.app_settings.update_one(
        {"_id": SETTINGS_ID},
        {
            "$set": {
                "provider": settings.provider,
                "model": settings.model.strip(),
                "ollama_host": settings.ollama_host,
                "base_url": (
                    settings.base_url if settings.provider == "custom" else None
                ),
                "updated_at": datetime.now(timezone.utc),
            }
        },
        upsert=True,
    )


def create_provider_from_settings(
    settings: AISettings,
) -> AIProvider:
    """Erzeugt den ausgewählten KI-Provider."""

    validate_ai_settings(settings)

    return create_ai_provider(
        provider_name=settings.provider,
        model=settings.model,
        ollama_host=settings.ollama_host,
        api_key=settings.api_key,
        base_url=settings.base_url,
    )


def settings_identity(settings: AISettings) -> str:
    """Cache-/Konfigurationskennung ohne API-Schlüssel."""

    endpoint = (
        settings.base_url
        if settings.provider == "custom"
        else settings.ollama_host
        if settings.provider == "ollama"
        else "https://api.openai.com/v1"
    )

    # Einheitliche URL ohne abschließenden Slash.
    endpoint = (endpoint or "").rstrip("/")

    # Bestehenden Fingerprint um Anbieter und Endpoint ergänzen.
    source = f"{settings.provider}|{settings.model}|{endpoint}"

    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def change_model(
    settings: AISettings,
    model: str,
) -> AISettings:
    """Hilfsfunktion für spätere Erweiterungen."""

    return replace(settings, model=model)
