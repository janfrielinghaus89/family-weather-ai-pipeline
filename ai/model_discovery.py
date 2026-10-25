import httpx


def list_ollama_models(host: str) -> list[str]:
    """Lädt die lokal installierten Ollama-Modelle."""

    response = httpx.get(
        f"{host.rstrip('/')}/api/tags",
        timeout=5,
        trust_env=False,
    )

    response.raise_for_status()

    models = response.json().get("models", [])

    return sorted({model["name"] for model in models if model.get("name")})
