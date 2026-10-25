import streamlit as st

from ai.configuration import AISettings
from ai.model_discovery import list_ollama_models
from ai.settings import (
    create_provider_from_settings,
    load_ai_settings,
    save_ai_settings,
)


def render_ai_settings(db) -> None:
    """Zeigt die Einstellungen für den aktiven KI-Anbieter."""

    st.subheader("AI Settings")

    current = load_ai_settings(db)

    provider_options = ["ollama", "openai", "custom"]

    with st.form("ai_settings_form"):
        provider = st.selectbox(
            "KI-Anbieter",
            options=provider_options,
            index=provider_options.index(current.provider),
            format_func=lambda value: {
                "ollama": "Ollama (lokal)",
                "openai": "OpenAI API",
                "custom": "Custom OpenAI-compatible API",
            }[value],
        )

        ollama_host = st.text_input(
            "Ollama Host",
            value=current.ollama_host,
            disabled=provider != "ollama",
        )

        models = []

        if provider == "ollama":
            try:
                models = list_ollama_models(ollama_host)
            except (OSError, ValueError, KeyError) as error:
                st.warning(f"Ollama-Modelle konnten nicht geladen werden: {error}")

        if provider == "ollama" and models:
            if current.provider == provider and current.model in models:
                selected_index = models.index(current.model)
            else:
                selected_index = 0

            model = st.selectbox(
                "Installiertes Modell",
                models,
                index=selected_index,
            )
        else:
            model = st.text_input(
                "Modellkennung",
                value=(current.model if provider == current.provider else ""),
                placeholder="Modell-ID eingeben",
            )

        base_url = st.text_input(
            "Custom API Base URL",
            value=current.base_url or "",
            disabled=provider != "custom",
            placeholder="https://api.example.com/v1",
        )

        if provider != "ollama":
            st.info(
                "API-Keys werden serverseitig über "
                "OPENAI_API_KEY oder CUSTOM_AI_API_KEY "
                "aus der .env geladen."
            )

        save = st.form_submit_button(
            "Einstellungen speichern",
            type="primary",
        )

    if save:
        from config import CUSTOM_AI_API_KEY, OPENAI_API_KEY

        api_key = {
            "ollama": None,
            "openai": OPENAI_API_KEY,
            "custom": CUSTOM_AI_API_KEY,
        }[provider]

        settings = AISettings(
            provider=provider,
            model=model,
            ollama_host=ollama_host,
            api_key=api_key,
            base_url=base_url if provider == "custom" else None,
        )

        try:
            save_ai_settings(db, settings)
        except ValueError as error:
            st.error(str(error))
        else:
            st.success(
                "Einstellungen gespeichert. "
                "Neue KI-Aufträge verwenden die neue Konfiguration."
            )
            st.rerun()

    if st.button("Verbindung testen"):
        try:
            provider_instance = create_provider_from_settings(current)

            from pydantic import BaseModel

            class TestResponse(BaseModel):
                status: str

            result = provider_instance.generate(
                system_prompt=("Antworte ausschließlich im verlangten JSON-Format."),
                user_prompt="Setze status auf OK.",
                response_model=TestResponse,
            )

            st.success(f"Verbindung erfolgreich: {result.status}")

        except Exception as error:
            st.error(f"Verbindungstest fehlgeschlagen: {error}")
