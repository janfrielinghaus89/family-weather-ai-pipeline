# WeatherWatcher — Family Weather AI Pipeline

**WeatherWatcher** is an open-source, event-driven weather application built with **Python, Apache Kafka, MongoDB, Streamlit, and configurable AI providers**. It collects forecasts from [Open-Meteo](https://open-meteo.com/), processes weather events, evaluates outdoor conditions with an LLM, and presents weather trends and personalized family recommendations in a local dashboard.

> **Status:** Local-first development prototype. The main workflows have been tested locally; this is not a production-hardened or multi-user service. AI-generated recommendations are informational, not safety guarantees.

## Highlights

- **Multiple locations:** Search by city or postal code, filter by country, and manage tracked locations in the dashboard.
- **Streaming pipeline:** Open-Meteo → Python producer → Kafka → Python consumer → MongoDB.
- **Weather assessment:** Structured AI sentiment score (1–10), short summary, and three family-friendly outdoor activities.
- **Family recommendations:** Profiles for 1–4 children, clothing suggestions and three age-appropriate activities.
- **Configurable AI:** Local **Ollama**, **OpenAI**, or a compatible custom API; select provider and model in **AI Settings**.
- **Inference caching:** MongoDB-backed weather fingerprint cache with a 60-minute reuse window, plus duplicate-event detection.
- **Resilience:** Input validation, retries with exponential backoff, dead-letter queue (DLQ), and manual Kafka offset commits.
- **History:** Interactive charts and filters for the last 24 hours, 7, 30, or 90 days.
- **Quality:** Pydantic validation, pytest, Ruff, and GitHub Actions CI.

## Architecture

```text
Open-Meteo Weather + Geocoding APIs
               |
       Python producer.py
    (tracked_locations in MongoDB)
               |
               v
       Kafka: weather_data
               |
               v
       Python consumer.py
               |
     Validate weather event
       |               |
       | invalid       +--> Kafka: weather_data_dlq
       v
 Check MongoDB duplicate ID
       |
 Read AI settings (MongoDB)
       |
 Check AI analysis cache
       |                  |
   Cache hit          Cache miss
       |                  |
       |             AI provider interface
       |            /       |          \
       |         Ollama   OpenAI     Custom API
       |                  |
       |             Cache response
       +---------+--------+
                 |
    MongoDB: weather_analysis
                 |
          Kafka offset commit
                 |
                 v
         Streamlit dashboard
        /         |          \
  Location     Weather       Family profile
  management    history          |
                                v
                       AI family recommendations
                                |
                    MongoDB family_recommendations
```

**Default deployment:** Kafka and MongoDB run in Podman containers. The Python applications and Ollama run on the host machine. Open-Meteo requests require internet access. **Selecting a cloud AI provider sends the weather inputs and, for personalized recommendations, family-profile information to that external service.** Ollama keeps LLM inference local.

## Requirements

- Python **3.11** (recommended for the current development setup)
- [Podman](https://podman.io/) and a working Compose provider (`podman compose`)
- [Ollama](https://ollama.com/) for local inference, **or** credentials for an external AI API
- Internet connection for Open-Meteo; external inference also requires API connectivity

Commands below target **Windows 11 / PowerShell**. On macOS/Linux, replace `.\.venv\Scripts\python.exe` with `.venv/bin/python` and adapt container startup as needed.

## Quick start (Windows / PowerShell)

### 1. Clone and prepare the environment

```powershell
git clone https://github.com/<YOUR-USERNAME>/<YOUR-REPOSITORY>.git
cd <YOUR-REPOSITORY>
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Edit `.env` with your local values. Example settings:

```dotenv
MONGO_USERNAME=root
MONGO_PASSWORD=CHANGE_ME
MONGO_HOST=127.0.0.1
MONGO_PORT=27017
KAFKA_BOOTSTRAP_SERVER=127.0.0.1:9092

AI_PROVIDER=ollama
AI_MODEL=
OLLAMA_HOST=http://127.0.0.1:11434
OLLAMA_MODEL=qwen3:8b

# Optional external-provider credentials (leave empty for Ollama)
OPENAI_API_KEY=
CUSTOM_AI_BASE_URL=
CUSTOM_AI_API_KEY=
```

Make sure `requirements.txt` includes `openai` alongside the existing project dependencies; the provider implementation uses the OpenAI Python SDK. The application uses `kafka-python-ng` for the producer and `confluent-kafka` for the consumer.

**Security:** Never commit `.env`, actual API keys, family data, or MongoDB exports. `.env.example` must contain placeholders only. Existing MongoDB users' passwords are **not** automatically changed when you edit Compose initialization variables.

### 2. Start Kafka and MongoDB

```powershell
podman machine start
podman compose up -d
podman compose ps
```

`podman machine start` may report that the machine is already running. The development setup uses the containers `local-kafka` and `local-mongodb` and exposes Kafka on port `9092` and MongoDB on port `27017`.

Create the Kafka topics if they do not exist:

```powershell
podman exec local-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic weather_data --partitions 1 --replication-factor 1
podman exec local-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic weather_data_dlq --partitions 1 --replication-factor 1
```

### 3. Prepare Ollama (default provider)

```powershell
ollama pull qwen3:8b
ollama list
Invoke-RestMethod -Uri "http://127.0.0.1:11434/api/tags" -Method Get
```

If Ollama is not running, start the application or run `ollama serve`. A second server is unnecessary if the Ollama application already listens on port `11434`.

The locally tested default is `qwen3:8b`. You can select another installed model in the dashboard's **AI Settings**. Response quality, memory requirements, and model speed vary. The Ollama adapter can disable thinking mode on compatible models to reduce latency.

### 4. Run the services

Start each command in a separate PowerShell terminal:

**Consumer** (Kafka → AI → MongoDB):

```powershell
.\.venv\Scripts\python.exe consumer.py
```

**Producer** (Open-Meteo → Kafka):

```powershell
.\.venv\Scripts\python.exe producer.py
```

The producer reads the locations saved in MongoDB (`tracked_locations`) and periodically publishes weather events; the development interval has been 300 seconds (5 minutes). Add at least one location in the dashboard to enable tracking. For a single polling cycle, the producer supports `--once`:

```powershell
.\.venv\Scripts\python.exe producer.py --once
```

**Dashboard:**

```powershell
.\.venv\Scripts\python.exe -m streamlit run dashboard.py
```

Open **http://localhost:8501**. Search for a location, add it to tracking, and allow the producer and consumer to populate its data. The dashboard reads stored records; its refresh button does not itself fetch forecasts.

## AI providers and settings

Open **⚙️ AI Settings** in the Streamlit sidebar to select the active provider and model. The configuration is persisted in MongoDB (`weather_project.app_settings`); the consumer checks for changes before processing a new weather event, so model changes do not require restarting it. An in-progress inference is not interrupted.

| Provider | Setup | Notes |
| --- | --- | --- |
| **Ollama** | Run Ollama and select a locally installed model | Local inference; no cloud API key required |
| **OpenAI** | Set `OPENAI_API_KEY`; select a supported model identifier | Uses structured responses via the official SDK; API usage may incur charges |
| **Custom API** | Set `CUSTOM_AI_API_KEY`, then configure an HTTPS base URL and model | Assumes an OpenAI-compatible chat endpoint with JSON response support; interoperability varies by service |

For external providers, configure secrets **on the server** in `.env` or your deployment's secret manager. The dashboard stores only non-secret settings. You can use **Test connection** to send a small real request; cloud tests may incur charges.

The application uses the same provider settings for general weather analysis and on-demand family recommendations. Provider, model, and endpoint identity are included in cache separation to prevent results from being reused across different configurations. The configuration identity does **not** include API keys.

> **Local-only administration:** The Settings page does not implement user authentication or role-based permissions. Do **not** expose the Streamlit dashboard to the public internet without access control. Only trusted administrators should be able to configure custom endpoints.

## Weather ingestion, reliability, and caching

### Incoming event validation

Events are checked with Pydantic before reaching the LLM. Invalid messages are sent to `weather_data_dlq`, including source metadata and the reason for rejection. The original event's Kafka offset is committed only after successful DLQ delivery. Since sending to the DLQ and committing the source offset are separate operations, duplicate DLQ records are possible during a crash or restart.

### At-least-once processing

The consumer uses explicit Kafka commits after successful MongoDB persistence, and its document IDs follow the format `topic:partition:offset`. Already persisted events can be skipped before inference. When transient AI failures exhaust their retries, the consumer stops without acknowledging the affected record, allowing recovery on restart.

This is **at-least-once**, not an end-to-end exactly-once guarantee. If the consumer is offline, Kafka lag may grow; old records should not be discarded casually in a real deployment.

### AI response caching

A SHA-256 weather fingerprint identifies identical conditions from the same location under the same AI configuration and prompt version. Cached responses are stored in `weather_project.ai_analysis_cache` and reused while fresh (up to **60 minutes**). A cache hit avoids another LLM call; a cache miss generates and stores a new response.

**New weather events still receive their own history records**, including when the analysis is reused from the cache. Logs show session-local cache hits, misses, hit rate, and inference duration. The cache currently compares exact values; small weather changes can cause misses.

The separate family recommendation cache is keyed by weather event, family profile, provider configuration, and recommendation version.

## Dashboard and family profiles

The dashboard provides:

- Current temperature, **forecast rain probability**, wind speed, weather sentiment, and three general outdoor activity suggestions.
- Search by location name or postal code, country filtering, saved-location management, and multi-location filtering.
- Historical Plotly charts for temperature, rain probability, and AI sentiment over the last **24 hours, 7, 30, or 90 days**.
- A local family profile with **1–4 children**, ages in months, optional genders, and preferred outdoor activities.
- Personalized clothing recommendations for each child and **exactly three** family activities; responses are validated using Pydantic and additional consistency checks.

Family recommendations are generated on demand and stored in `weather_project.family_recommendations`. Changing the profile or model configuration produces a different cache key.

**Important:** Rain probability is not measured rainfall. The sentiment score is an AI heuristic, not an official meteorological rating. AI-generated clothing/activity advice should be checked against actual weather and each child's health, age, and needs.

## Repository layout

```text
.
├── .github/
│   ├── workflows/ci.yml
│   └── PULL_REQUEST_TEMPLATE.md
├── ai/
│   ├── __init__.py
│   ├── base.py                  # Common provider interface
│   ├── ollama_provider.py       # Local inference
│   ├── openai_provider.py       # OpenAI / custom API inference
│   ├── factory.py               # Provider creation
│   ├── configuration.py         # Environment defaults
│   ├── settings.py              # Persisted settings / identity
│   ├── model_discovery.py       # Ollama model discovery
│   └── settings_ui.py           # Streamlit settings UI
├── tests/
├── ai_cache.py                  # Weather fingerprints and cache
├── pipeline_resilience.py       # Validation, retries, DLQ and duplicates
├── location_service.py          # Geocoding and tracked locations
├── producer.py                  # Open-Meteo → Kafka
├── consumer.py                  # Kafka → provider → MongoDB
├── family.py                    # Family profiles and recommendations
├── dashboard.py                 # Streamlit dashboard
├── weather_utils.py
├── config.py
├── docker-compose.yml
├── .env.example
├── requirements.txt
├── requirements-dev.txt
├── pyproject.toml
├── CONTRIBUTING.md
├── LICENSE
└── README.md
```

## MongoDB collections

| Collection (`weather_project`) | Purpose |
| --- | --- |
| `tracked_locations` | User-managed locations and Open-Meteo identifiers |
| `weather_analysis` | Original weather events, AI output, Kafka metadata |
| `ai_analysis_cache` | Cached general AI weather analyses |
| `family_profiles` | Local family preferences and child ages |
| `family_recommendations` | Cached personalized advice |
| `app_settings` | Active non-secret provider/model configuration |

Example weather event (simplified):

```json
{
  "location_id": 2950159,
  "city": "Berlin",
  "timestamp": "2026-10-23T11:30:02+00:00",
  "temperature_c": 12.5,
  "rain_probability_percent": 40,
  "wind_speed_kmh": 15.3,
  "weather_code": 3
}
```

The `location_id` above is illustrative. Historical charts currently use the AI **processing timestamp**, which can differ from the weather observation or forecast time.

## Verification and development

Install developer dependencies:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
```

Run the same checks used by CI:

```powershell
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m ruff format --check .
.\.venv\Scripts\python.exe -m pytest -q
```

To apply supported lint and formatting fixes locally:

```powershell
.\.venv\Scripts\python.exe -m ruff check . --fix
.\.venv\Scripts\python.exe -m ruff format .
```

Tests are designed to cover provider behavior, settings, cache key separation, retry logic, duplicate detection, and other pure Python functionality without requiring access to real cloud keys. Full end-to-end tests involving the running Kafka/MongoDB/Ollama services are currently performed manually.

### Inspect Kafka consumer lag

```powershell
podman exec local-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group weather-ai-consumer
```

`LAG` is the difference between the topic's log end and the last committed group offset. A lag of zero is desirable in steady operation, but may temporarily rise during an expensive AI inference.

### Inspect the dead-letter queue

```powershell
podman exec -it local-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic weather_data_dlq --from-beginning
```

### Inspect database documents

```powershell
podman exec -it local-mongodb mongosh -u root --authenticationDatabase admin
```

Inside `mongosh`:

```javascript
use weather_project
db.weather_analysis.find().sort({"metadata.processed_at": -1}).limit(5).pretty()
db.app_settings.find().pretty()
db.family_recommendations.find().sort({"created_at": -1}).limit(3).pretty()
```

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Podman services will not start | `podman machine list`, `podman compose ps`, `podman logs local-kafka` |
| Kafka connection errors | Container health, `KAFKA_BOOTSTRAP_SERVER`, advertised listeners |
| Kafka lag keeps increasing | Consumer logs, Ollama/API speed, cache hit rate; verify producer frequency |
| No tracked weather appears | Add a location, run producer and consumer, inspect Kafka topic |
| Ollama connection fails | `Invoke-RestMethod -Uri "http://127.0.0.1:11434/api/tags" -Method Get` |
| Ollama inference times out | `ollama ps`, model availability, GPU/CPU usage and provider timeout; long thinking output can increase latency |
| Cloud AI authentication fails | Check server-side API key and the selected provider/model; never log secrets |
| Custom endpoint fails | Verify HTTPS URL and the provider's compatibility with chat completions and JSON output |
| Weather/family recommendation seems stale | Check provider selection, source weather event, profile and the corresponding cache key |
| MongoDB authentication fails | Check credentials and existing persisted MongoDB users |
| Ruff or CI fails | Run lint, formatting and pytest locally; avoid importing secret-dependent application entrypoints in pure unit tests |

## Security and privacy

- Keep `.env` and any secrets out of version control; commit only sanitized `.env.example` values.
- `app_settings` intentionally stores **no API keys**. Keys are resolved from the application environment.
- Cloud inference transmits prompts (potentially including a family profile) to the selected external provider; check that provider's policies before enabling it.
- Do not expose MongoDB, Kafka, or the unauthenticated dashboard to untrusted networks.
- Limit custom API destinations to trusted endpoints in any shared or deployed installation.
- This prototype uses one local family profile (`local-family`) and does **not** implement per-user accounts, permissions, or encrypted secret administration.

## Stop services

Stop producer, consumer, and Streamlit with `Ctrl+C`, then run:

```powershell
podman compose down
```

This normally keeps persistent data volumes. **Do not use `podman compose down -v`** unless you intend to remove persistent container data.

## Roadmap

- [x] Multi-location search and tracking
- [x] Historical weather and AI sentiment dashboards
- [x] Family profiles and age-aware recommendations
- [x] Weather event validation, retries, and DLQ
- [x] Idempotent Kafka-to-MongoDB processing
- [x] AI response caching and duplicate detection
- [x] Configurable local and external AI providers
- [x] Persistent AI settings and Ollama model selection
- [x] Tests, Ruff, GitHub Actions, MIT license and contribution guidelines
- [ ] Automated end-to-end/integration tests for containers and external services
- [ ] Persistent inference metrics and Kafka-lag monitoring in the dashboard
- [ ] Stronger endpoint restrictions, authentication, and multi-user support
- [ ] Optional per-feature AI model configuration

## Contributing and license

Issues and pull requests are welcome. Please see [CONTRIBUTING.md](CONTRIBUTING.md) before contributing. Distributed under the [MIT License](LICENSE).

## Data sources and responsible use

Weather forecasts and geocoding use [Open-Meteo](https://open-meteo.com/); follow the provider's current attribution, licensing, and request-limit requirements. AI results may contain errors. Always check observed conditions and use independent judgment for safety-related decisions.
