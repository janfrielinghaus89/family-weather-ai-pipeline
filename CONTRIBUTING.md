# Contributing to WeatherWatcher

Thank you for your interest in contributing!

## Getting started

1. Fork the repository and clone your fork.
2. Create a branch from an up-to-date `main` branch (`feat/`, `fix/`, `chore/`, or `docs/`).
3. Set up Python 3.11 and a virtual environment.
4. Install dependencies: `python -m pip install -r requirements-dev.txt`.
5. Copy `.env.example` to `.env` and configure local settings. Never commit `.env` or credentials.

## Quality checks

Run before opening a pull request:

```bash
python -m ruff check .
python -m ruff format --check .
python -m pytest -q
```

The unit tests do not require running Kafka, MongoDB, or Ollama.

## Pull requests

- Prefer small, focused pull requests.
- Describe the motivation and changes.
- Add or update tests for behavioral changes.
- Update documentation when setup or features change.
- Avoid unrelated refactoring in feature PRs.

## Reporting issues

Include reproduction steps, expected and actual behavior, operating system, Python version and relevant logs. Remove credentials and personal data from logs.
