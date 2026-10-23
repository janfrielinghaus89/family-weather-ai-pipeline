import os

from dotenv import load_dotenv

load_dotenv(override=True)

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434")

OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:latest")
