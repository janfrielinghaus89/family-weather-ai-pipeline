from typing import TypeVar

import ollama
from pydantic import BaseModel

from ai.base import AIProvider

T = TypeVar("T", bound=BaseModel)


class OllamaProvider(AIProvider):
    def __init__(
        self,
        host: str,
        model: str,
        timeout: float = 300,
    ) -> None:
        self._model = model
        self._client = ollama.Client(
            host=host,
            timeout=timeout,
            trust_env=False,
        )

    @property
    def model_name(self) -> str:
        return self._model

    @property
    def provider_name(self) -> str:
        return "ollama"

    def generate(
        self,
        system_prompt: str,
        user_prompt: str,
        response_model: type[T],
    ) -> T:
        response = self._client.chat(
            model=self._model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            format=response_model.model_json_schema(),
            options={
                "temperature": 0.2,
                "num_predict": 768,
            },
            think=False,
            stream=False,
        )

        return response_model.model_validate_json(response["message"]["content"])
