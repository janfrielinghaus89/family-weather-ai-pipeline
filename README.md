# WeatherWatcher — Family Weather AI Pipeline

A local, event-driven weather application built with **Python, Apache Kafka, MongoDB, Ollama, and Streamlit**. WeatherWatcher fetches forecasts from [Open-Meteo](https://open-meteo.com/), streams weather events through Kafka, analyzes them with a locally running LLM, and displays weather conditions, sentiment, historical trends, and personalized family recommendations in an interactive dashboard.

> **Project status:** Active local prototype. Features and setup instructions will evolve as the project grows.

## Architecture

```text
                    Open-Meteo API
                          |
                   Python Producer
                  (configurable city)
                          |
                    JSON events
                          v
                 Apache Kafka (KRaft)
                   topic: weather_data
                          |
                   Python Consumer
                          | \
                          |  +------> Ollama (local LLM)
                          |           weather assessment
                          v
                MongoDB: weather_analysis
                    /             \
                   /               \
        Historical data          Latest weather
                 \                  /
                  v                v
                    Streamlit Dashboard
                       |         |
              Family profile      |----> Ollama (local LLM)
                (MongoDB)         |      personalized advice
                       |          |              |
                       +----------+--------------+
                                  |
                        MongoDB: family_recommendations
                                  |
                                Browser
```

**Deployment model:** Kafka and MongoDB run in Podman containers; the producer, consumer, Streamlit, and Ollama run locally on the host. Weather API requests go to Open-Meteo, while LLM inference and family-profile processing remain local.

## Features

### Weather ingestion and AI processing

- Look up coordinates for a city using Open-Meteo geocoding.
- Publish temperature, hourly precipitation probability, wind speed, and weather code as JSON to Kafka's `weather_data` topic.
- Generate a weather sentiment score (**1–10**), a summary, and three general family-friendly outdoor recommendations using a local Ollama model.
- Store the original weather event, AI response, and Kafka processing metadata in `weather_project.weather_analysis`.
- Commit Kafka offsets only after MongoDB persistence; use the Kafka topic, partition, and offset as a stable document ID to avoid duplicate inserts on replay.
- Use **`confluent-kafka`** for the consumer on Windows; the current producer uses **`kafka-python-ng`**.

### Streamlit dashboard

- Show the latest stored temperature, rain probability, wind speed, and color-coded weather sentiment.
- Select a city dynamically from those already present in MongoDB; optionally show the latest record across all cities.
- Manually refresh the dashboard without triggering a new Open-Meteo request.
- Explore interactive Plotly charts for temperature, rain probability, and AI sentiment.
- Filter historical data by city and the last **24 hours, 7, 30, or 90 days**, including multi-city comparisons.
- View historical statistics and an expandable records table.

### Family profiles and personalized recommendations

- Save a persistent local family profile in `weather_project.family_profiles`.
- Configure **1–4 children**, their ages in months, optional gender information, and preferred activity categories.
- Generate age- and weather-aware clothing guidance for each child and **three personalized outdoor activity suggestions** using Ollama.
- Apply basic weather and developmental-stage context in Python before prompting the model.
- Validate structured model responses with Pydantic and retry invalid responses (up to three attempts).
- Cache generated results in `weather_project.family_recommendations` for the same weather event, profile, model, and recommendation version.
- Keep family personalization separate from the Kafka weather-ingestion pipeline.

> **Note:** This version uses a single local family profile (`local-family`), not multi-user authentication. Recommendations are informational and must be checked against actual conditions and the child's needs. Local model output can be imperfect.

### Location Search & Multi-Location Tracking

WeatherWatcher supports dynamic location management using the Open-Meteo Geocoding API.

Features include:
- Search for locations by city name or postal code.
- Filter search results by country.
- Add and remove tracked locations through the Streamlit dashboard.
- Persist tracked locations in MongoDB.
- Automatically fetch weather data for all tracked locations using a single producer.
- Identify locations using unique Open-Meteo IDs.
- Filter current weather and historical analytics by selected location.

Tracked locations are managed independently of historical weather records. Removing a location stops future tracking without deleting existing weather data.

### Pipeline Resilience & Error Handling

WeatherWatcher includes fault-tolerant processing mechanisms to improve reliability when handling weather events and communicating with external services.

Key features:

- **Weather Data Validation:** Incoming Kafka messages are validated with Pydantic before processing.
- **Retry Mechanism:** Temporary Ollama failures trigger retries with exponential backoff.
- **Dead Letter Queue:** Invalid weather events are redirected to the `weather_data_dlq` Kafka topic for investigation.
- **Manual Offset Management:** Kafka offsets are committed only after successful MongoDB persistence or confirmed DLQ delivery.
- **Idempotent Storage:** MongoDB document IDs derived from Kafka topic, partition, and offset help prevent duplicate records.
- **Error Logging:** Invalid messages, retry attempts, and processing failures are logged for troubleshooting.

If retries are exhausted during an Ollama or MongoDB-related failure, the consumer stops without acknowledging the affected message. Processing can resume from the last committed Kafka offset when the consumer restarts.

The current implementation provides at-least-once processing behavior, rather than end-to-end exactly-once guarantees.

### AI Inference Optimization

WeatherWatcher minimizes redundant LLM inference through a MongoDB-backed caching mechanism.

**Features:**
- **Weather Fingerprinting:** Generates deterministic SHA-256 hashes based on location, weather conditions, AI model, and prompt version.
- **AI Response Caching:** Reuses previously generated weather analyses for identical conditions.
- **Cache Expiration:** Cached analyses remain reusable for up to 60 minutes.
- **Duplicate Detection:** Already persisted Kafka messages are skipped before AI processing.
- **Performance Logging:** Tracks cache hits, misses, hit rates, and inference duration.
- **Historical Data Preservation:** Every new valid Kafka event is still stored, even when its AI analysis comes from the cache.

These optimizations significantly reduce unnecessary LLM requests and help prevent Kafka consumer lag when using local AI models such as Qwen3 8B.

Cache hits and misses are currently tracked per consumer session. Persistent performance monitoring is planned for a future release.

## Repository layout

```text
.
├── .github/
│   ├── workflows/ci.yml       # GitHub Actions checks
│   └── PULL_REQUEST_TEMPLATE.md
├── tests/                      # pytest unit tests
├── .env.example                # Example environment settings (no secrets)
├── .gitignore
├── config.py                   # Shared configuration for local services
├── docker-compose.yml         # Kafka (KRaft) and MongoDB via Podman
├── producer.py                # Open-Meteo → Kafka
├── consumer.py                # Kafka → Ollama → MongoDB
├── family.py                  # Family profiles and personalized advice
├── dashboard.py               # Streamlit UI and historical analytics
├── weather_utils.py           # Reusable/testable formatting utilities
├── pyproject.toml             # Quality-tool configuration
├── requirements.txt
├── requirements-dev.txt
├── CONTRIBUTING.md
├── LICENSE
└── README.md
```

## Prerequisites

- **Python 3.11** recommended (use a Python version supported by the dependency versions in this repository).
- **Podman** with a working Compose provider (`podman compose`). On Windows, Podman Machine / WSL2 is typically used.
- **Ollama** installed locally.
- Internet access to query Open-Meteo.

The commands below target **Windows 11 / PowerShell** (including VS Code's terminal). For macOS/Linux, use `.venv/bin/python` instead of `.\.venv\Scripts\python.exe`.

## Quick start (Windows / PowerShell)

### 1. Clone and configure

```powershell
git clone https://github.com/<YOUR-USERNAME>/<YOUR-REPOSITORY>.git
cd <YOUR-REPOSITORY>
Copy-Item .env.example .env
```

Edit `.env` for your local machine. The current development configuration uses settings like:

```dotenv
MONGO_USERNAME=root
MONGO_PASSWORD=CHANGE_ME
MONGO_DATABASE=weather_project
MONGO_HOST=127.0.0.1
MONGO_PORT=27017
KAFKA_BOOTSTRAP_SERVER=127.0.0.1:9092
OLLAMA_HOST=http://127.0.0.1:11434
OLLAMA_MODEL=qwen3:8b
```

Choose a local password for `MONGO_PASSWORD`, and ensure `docker-compose.yml`, `consumer.py`, and `dashboard.py` all use the corresponding environment settings. The `.env` file **must not** be committed. If you already have a persisted MongoDB volume, changing the initialization password in `.env` does **not** update the existing database user's password.

### 2. Start Podman services

```powershell
podman machine start
podman compose up -d
podman compose ps
```

If Podman Machine is already running, `podman machine start` may indicate that no startup is needed. The expected containers are `local-kafka` (host port `9092`) and `local-mongodb` (host port `27017`). Kafka runs in single-node **KRaft** mode without ZooKeeper.

Create and inspect the Kafka topic:

```powershell
podman exec local-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic weather_data --partitions 1 --replication-factor 1
podman exec local-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### 3. Install Python dependencies

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

The application uses `requests`, `kafka-python-ng` (producer), `confluent-kafka` (consumer), `pymongo`, `ollama`, `pydantic` v2, `python-dotenv`, `streamlit`, `pandas`, and `plotly`. Ensure `requirements.txt` reflects these packages.

### 4. Prepare Ollama

The current recommended model for personalized recommendations is **Qwen3 8B**. Download it with:

```powershell
ollama pull qwen3:8b
ollama list
```

If your setup needs Ollama started explicitly, run `ollama serve` in another terminal. On Windows, the Ollama application may already be serving in the background; a port-in-use message means you should check the existing service, not start a second instance.

Verify connectivity:

```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:11434/api/tags" -Method Get
```

You can choose a different installed Ollama model through `OLLAMA_MODEL`. Larger models can produce better recommendations but generally require more memory and time. **Model behavior is not guaranteed**; the application validates response structure but cannot guarantee suitability of every suggestion.

### 5. Start the consumer

In one terminal:

```powershell
.\.venv\Scripts\python.exe consumer.py
```

The consumer reads from `weather_data`, generates general weather analysis via Ollama, and stores processed events in MongoDB.

### 6. Start one or more producers

In a second terminal:

```powershell
.\.venv\Scripts\python.exe producer.py --city Berlin
```

For another city, run a separate producer process, for example:

```powershell
.\.venv\Scripts\python.exe producer.py --city Lippstadt
```

For this local prototype, an interval of **300 seconds (5 minutes)** is a reasonable development default. Check `INTERVAL_SECONDS` in `producer.py` to confirm or change the actual polling interval.

### 7. Launch Streamlit

In a third terminal:

```powershell
.\.venv\Scripts\python.exe -m streamlit run dashboard.py
```

Open **http://localhost:8501**.

In the dashboard, select a city, inspect the current weather and historical trends, configure and save your family profile, then select **Generate recommendations** (the UI may display the German label **“Empfehlungen generieren”**). Personalized advice is generated on demand and cached in MongoDB. A manual weather-data refresh only re-reads stored records.

## Data storage

| MongoDB collection | Purpose |
| --- | --- |
| `weather_project.weather_analysis` | Raw weather data, general AI sentiment and recommendations, and Kafka metadata |
| `weather_project.family_profiles` | Persistent configuration for the local family |
| `weather_project.family_recommendations` | Cached, profile-specific clothing and activity recommendations |

Example Kafka weather event (simplified):

```json
{
  "city": "Berlin",
  "timestamp": "2026-10-23T11:30:02+00:00",
  "temperature_c": 12.5,
  "rain_probability_percent": 40,
  "wind_speed_kmh": 15.3,
  "weather_code": 3
}
```

`rain_probability_percent` is an **hourly forecast probability**, not measured rainfall. AI weather sentiment is a heuristic generated by the model, not an official meteorological index. Historical graphs currently use the **AI processing timestamp**, which may differ from the weather observation time.

## Verify the pipeline

Check the containers:

```powershell
podman ps
```

Inspect Kafka events (stop with `Ctrl+C`):

```powershell
podman exec -it local-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic weather_data --from-beginning
```

Inspect MongoDB documents (enter the configured password when prompted):

```powershell
podman exec -it local-mongodb mongosh -u root --authenticationDatabase admin
```

Then, inside `mongosh`:

```javascript
use weather_project
db.weather_analysis.find().sort({"metadata.processed_at": -1}).limit(5).pretty()
db.family_profiles.find().pretty()
db.family_recommendations.find().sort({"created_at": -1}).limit(3).pretty()
```

## Configuration and security

Keep credentials and machine-specific connection settings in an ignored `.env` file. Commit only `.env.example` with non-secret placeholders. Shared Ollama settings are loaded via `config.py`; other modules should likewise use environment-based configuration rather than hard-coded credentials.

| Variable / setting | Purpose | Example |
| --- | --- | --- |
| `MONGO_USERNAME` | MongoDB user | `root` (local development) |
| `MONGO_PASSWORD` | MongoDB password | `CHANGE_ME` in `.env.example` |
| `MONGO_DATABASE` | Database name | `weather_project` |
| `MONGO_HOST`, `MONGO_PORT` | Host-side MongoDB connection | `127.0.0.1`, `27017` |
| `KAFKA_BOOTSTRAP_SERVER` | Host-side Kafka connection | `127.0.0.1:9092` |
| `OLLAMA_HOST` | Local Ollama API endpoint | `http://127.0.0.1:11434` |
| `OLLAMA_MODEL` | Locally installed model | `qwen3:8b` |
| `INTERVAL_SECONDS` in `producer.py` | Producer polling interval | `300` |
| Producer `--city` | Requested city | `Berlin` |

**Security notes:** Do not commit `.env`, credentials, personal family information, or database dumps. The current family profile is local and shared by anyone able to use the dashboard; **do not expose the dashboard publicly without authentication and access controls**. Avoid exposing Kafka and MongoDB ports to untrusted networks. This project is a development prototype, not a hardened hosted service.

## Development & quality assurance

Install development dependencies:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
```

Run the same checks expected in GitHub Actions:

```powershell
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m ruff format --check .
.\.venv\Scripts\python.exe -m pytest -q
```

Quality checks run automatically on configured GitHub Actions triggers. Unit tests cover selected helper and family-model logic; full integration testing of Podman, Kafka, MongoDB, and Ollama remains future work.

Contributions are welcome: read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a PR. See the [MIT License](LICENSE) for licensing terms.

## Troubleshooting

| Symptom | First checks |
| --- | --- |
| Podman exit code `125` | `podman machine list`, `podman --log-level=debug machine start`, and on Windows `wsl --list --verbose` |
| Kafka connection errors | `podman compose ps`, `podman logs local-kafka`; check the `KAFKA_BOOTSTRAP_SERVER` value |
| Ollama connection error | `Invoke-RestMethod http://127.0.0.1:11434/api/tags`; confirm the effective `OLLAMA_HOST` is a client URL, not `0.0.0.0` |
| Incorrect model selected | `ollama list` and the `OLLAMA_MODEL` setting in `.env` |
| MongoDB authentication failure | Check credentials and remember that changing Compose initialization variables does not modify existing volume users |
| Dashboard recommendations remain unchanged | Personalized results are cached: check the weather event, profile, selected model, and `RECOMMENDATION_VERSION` in `family.py` |
| Dashboard shows outdated weather | Start the producer and consumer; dashboard refresh does not retrieve new forecasts |
| Invalid AI response | Check Pydantic validation logs; the model may need retries or a more specific prompt |

## Stop the services

Stop Python processes with `Ctrl+C`. Then stop the containers:

```powershell
podman compose down
```

This normally preserves the MongoDB named volume. **Do not use `podman compose down -v` unless you intend to remove persistent data.**

## Roadmap

- [x] Move credentials and connection settings to `.env` / environment variables.
- [x] Add city selection and filtering in the dashboard.
- [x] Add historical weather/sentiment graphs.
- [x] Add AI-based clothing suggestions for children.
- [x] Add a text search for a specific town (including postal codes).
- [x] Add family configuration: number of children, ages, optional gender, and activity preferences.
- [x] Add tests, linting, and CI.
- [ ] Improve resilience for Ollama outages and invalid records.
- [x] Add a license and contribution guidelines.

## Data sources and responsible use

Weather and geocoding data come from [Open-Meteo](https://open-meteo.com/). Respect its applicable usage terms, attribution rules, and rate limits. Forecasts and LLM-generated advice are not guaranteed to be accurate. For children, adapt clothing and outdoor plans to observed conditions, developmental abilities, and individual needs.
