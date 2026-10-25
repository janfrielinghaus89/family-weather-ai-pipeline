import os

from dotenv import load_dotenv

load_dotenv(override=True)

AI_PROVIDER = os.getenv("AI_PROVIDER", "ollama").lower()

AI_MODEL = os.getenv("AI_MODEL", "")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
CUSTOM_AI_BASE_URL = os.getenv("CUSTOM_AI_BASE_URL", "")
CUSTOM_AI_API_KEY = os.getenv("CUSTOM_AI_API_KEY", "")

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:latest")
