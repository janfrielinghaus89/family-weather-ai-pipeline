from typing import TypeVar

from openai import OpenAI
from pydantic import BaseModel

from ai.base import AIProvider

T = TypeVar("T", bound=BaseModel)


class OpenAIProvider(AIProvider):
    """Provider für OpenAI und kompatible Chat-APIs."""

    def __init__(
        self,
        model: str,
        api_key: str,
        base_url: str | None = None,
        custom: bool = False,
        timeout: float = 120,
    ) -> None:
        if not api_key:
            raise ValueError("API key is required")

        if not model:
            raise ValueError("AI model is required")

        if custom and not base_url:
            raise ValueError("Custom API requires a base URL")

        self._model = model
        self._custom = custom

        self._client = OpenAI(
            api_key=api_key,
            base_url=base_url,
            timeout=timeout,
            max_retries=0,
        )

    @property
    def model_name(self) -> str:
        return self._model

    @property
    def provider_name(self) -> str:
        return "custom" if self._custom else "openai"

    def generate(
        self,
        system_prompt: str,
        user_prompt: str,
        response_model: type[T],
    ) -> T:
        messages = [
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "user",
                "content": user_prompt,
            },
        ]

        if self._custom:
            # Breitere Kompatibilität mit Drittanbietern:
            # JSON-Modus + lokale Pydantic-Validierung.
            response = self._client.chat.completions.create(
                model=self._model,
                messages=messages,
                response_format={"type": "json_object"},
            )

            content = response.choices[0].message.content

            if not content:
                raise ValueError("AI provider returned an empty response")

            return response_model.model_validate_json(content)

        # Offizielle OpenAI API:
        # Strukturierte Ausgabe direkt als Pydantic-Modell.
        response = self._client.chat.completions.parse(
            model=self._model,
            messages=messages,
            response_format=response_model,
        )

        message = response.choices[0].message

        if message.refusal:
            raise ValueError(f"AI provider refused the request: {message.refusal}")

        if message.parsed is None:
            raise ValueError("AI provider returned no structured response")

        return message.parsed
