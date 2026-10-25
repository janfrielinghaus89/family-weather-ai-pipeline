from abc import ABC, abstractmethod
from typing import TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class AIProvider(ABC):
    @abstractmethod
    def generate(
        self,
        system_prompt: str,
        user_prompt: str,
        response_model: type[T],
    ) -> T:
        """Erzeugt eine validierte, strukturierte KI-Antwort."""
        ...

    @property
    @abstractmethod
    def model_name(self) -> str:
        """Aktuell verwendetes Modell."""
        ...

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Name des KI-Anbieters."""
        ...
