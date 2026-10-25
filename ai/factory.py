from ai.base import AIProvider
from ai.ollama_provider import OllamaProvider
from ai.openai_provider import OpenAIProvider


def create_ai_provider(
    provider_name: str,
    model: str,
    ollama_host: str,
    api_key: str | None = None,
    base_url: str | None = None,
) -> AIProvider:
    """Erstellt den gewünschten KI-Provider."""

    provider_name = provider_name.strip().lower()

    if provider_name == "ollama":
        return OllamaProvider(
            host=ollama_host,
            model=model,
        )

    if provider_name == "openai":
        return OpenAIProvider(
            model=model,
            api_key=api_key or "",
        )

    if provider_name == "custom":
        return OpenAIProvider(
            model=model,
            api_key=api_key or "",
            base_url=base_url,
            custom=True,
        )

    raise ValueError(f"Unsupported AI provider: {provider_name}")
