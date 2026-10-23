# Family Weather AI Pipeline

A local, event-driven weather data pipeline built with **Python, Apache Kafka, MongoDB, Ollama, and Streamlit**. It retrieves weather information from [Open-Meteo](https://open-meteo.com/), streams JSON events through Kafka, generates family-friendly weather assessments with a local LLM, persists the results in MongoDB, and displays them in a dashboard.

> **Project status:** Working local prototype. This README will grow as the project evolves.

## Architecture

```text
                         every 5 minutes (configurable)
 [Open-Meteo API] --------------------------------------+
                                                       |
                                                       v
                                               [Python Producer]
                                                       |
                                                       | JSON: weather_data
                                                       v
                                                 [Apache Kafka]
                                                       |
                                                       v
                                               [Python Consumer]
                                                  |         |
                                     prompt/JSON  |         |  structured record
                                                  v         v
                                           [Ollama llama3.2] [MongoDB]
                                                                |
                                                                v
                                                        [Streamlit UI]
                                                                |
                                                                v
                                                            [Browser]
```

Kafka and MongoDB run in **Podman** containers. The Python applications and Ollama run locally on the host. All LLM inference stays on the local machine; weather information is fetched from Open-Meteo.

## Features

- Fetches weather for a configurable city, using Open-Meteo geocoding and forecast APIs.
- Publishes temperature, hourly precipitation probability, wind speed, and weather code to the Kafka topic `weather_data`.
- Uses local `llama3.2` via the official `ollama` Python package to produce a weather sentiment score from **1–10** and **exactly three** family-friendly outdoor recommendations.
- Stores raw weather observations, structured AI output, and processing metadata in MongoDB.
- Shows the latest processed observation in a Streamlit dashboard, with manual refresh.
- Commits Kafka offsets after successful database persistence; uses topic/partition/offset as a stable MongoDB document ID to avoid duplicates on replay.

### Historical Weather Analytics

The Streamlit dashboard provides interactive historical weather analytics powered by **Plotly and MongoDB**.

Features include:
- **Temperature History:** Interactive charts displaying temperature changes over time.
- **Precipitation Probability:** Historical rain probability visualization.
- **AI Weather Sentiment:** Track AI-generated weather sentiment scores (1–10).
- **Time Range Selection:** Filter data by the last 24 hours, 7 days, 30 days, or 90 days.
- **City Filtering:** View historical data for individual cities or compare multiple cities.
- **Historical Statistics:** Display average temperature, average sentiment, and the number of processed weather records.
- **Data Exploration:** Expandable table displaying historical weather records.

All visualizations use previously processed weather data stored in MongoDB. Charts update when the selected city or time range changes.

## Repository layout

```text
.
├── docker-compose.yml      # Kafka (KRaft) and MongoDB for Podman
├── producer.py             # Open-Meteo → Kafka
├── consumer.py             # Kafka → Ollama → MongoDB
├── dashboard.py            # MongoDB → Streamlit
├── requirements.txt        # Python dependencies
├── .gitignore              # Local-only and sensitive files
└── README.md
```

## Prerequisites

- **Python 3.10+** (a version supported by your chosen package releases)
- **Podman** and a Compose provider (`podman compose`)
- **Ollama** installed and accessible at `http://localhost:11434`
- Internet access for Open-Meteo requests

The examples below target **Windows 11 / PowerShell** (including the integrated terminal in VS Code). On macOS or Linux, use your shell's virtual-environment activation or invoke `.venv/bin/python` instead.

## Quick start

### 1. Clone the repository

```powershell
git clone https://github.com/<YOUR-USERNAME>/<YOUR-REPOSITORY>.git
cd <YOUR-REPOSITORY>
```

Replace the placeholders after publishing the repository.

### 2. Start Kafka and MongoDB with Podman

If using Podman Machine on Windows, start it first:

```powershell
podman machine start
```

Then, from the repository root:

```powershell
podman compose up -d
podman compose ps
```

Expected services: `local-kafka` and `local-mongodb`. Kafka exposes port `9092`; MongoDB exposes port `27017` on the host. Your `docker-compose.yml` uses a single-node **KRaft** broker, so ZooKeeper is not required.

Create the Kafka topic (safe to rerun):

```powershell
podman exec local-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic weather_data --partitions 1 --replication-factor 1
```

Verify the topic:

```powershell
podman exec local-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

> **Security note:** The initial Compose setup uses a development-only MongoDB root password. Before pushing to a **public repository**, remove credentials from tracked files and load them from an ignored `.env` file or environment variables instead. Never expose these local ports to untrusted networks.

### 3. Install Python dependencies

From the repository root:

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

The application requires `requests`, `kafka-python-ng`, `ollama`, `pydantic` (v2), `pymongo`, and `streamlit`.

### 4. Prepare Ollama

Install and start Ollama, then download the model:

```powershell
ollama pull llama3.2
ollama list
```

Ensure the local Ollama service is running. On systems where it does not run automatically, start it in a separate terminal with `ollama serve`.

### 5. Start the consumer

Open a VS Code PowerShell terminal:

```powershell
.\.venv\Scripts\python.exe consumer.py
```

The consumer processes messages from `weather_data`, requests structured JSON analysis from `llama3.2`, and writes to `weather_project.weather_analysis` in MongoDB.

### 6. Start the producer

Open **another** terminal:

```powershell
.\.venv\Scripts\python.exe producer.py --city Berlin
```

Or select another city:

```powershell
.\.venv\Scripts\python.exe producer.py --city München
```

The producer's `INTERVAL_SECONDS` value in `producer.py` controls the fetch interval. **Recommended for this demo: 300 seconds (5 minutes)**. The Open-Meteo values may update less often than the polling interval.

### 7. Launch the dashboard

Open a **third** terminal:

```powershell
.\.venv\Scripts\python.exe -m streamlit run dashboard.py
```

Visit **http://localhost:8501**. Click **“Daten aktualisieren”** to read the latest processed record from MongoDB. Refreshing the dashboard does not call Open-Meteo or Ollama; it only re-queries MongoDB.

## Example message and stored result

Simplified weather event published to Kafka:

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

The MongoDB document includes the complete `weather` payload, an `ai_analysis` object (`sentiment_score`, `sentiment_label`, `summary`, and three `activities`), and `metadata` (model, Kafka topic/partition/offset, and processing time).

> `rain_probability_percent` is the **hourly forecast probability**, not a measured rainfall amount. The sentiment score is an AI-generated heuristic, not an official meteorological index.

## Verify the pipeline

**Container status**:

```powershell
podman ps
```

**Inspect Kafka events** (stop with `Ctrl+C`):

```powershell
podman exec -it local-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic weather_data --from-beginning
```

**Inspect stored analyses**:

```powershell
podman exec -it local-mongodb mongosh -u root -p localpassword --authenticationDatabase admin
```

Inside `mongosh`:

```javascript
use weather_project
db.weather_analysis.find().sort({"metadata.processed_at": -1}).limit(5).pretty()
```

> Replace the example MongoDB password above if you have configured a different one. To avoid placing secrets in shell history, prefer prompting or securely provided environment variables.

## Configuration

| Setting | Where | Default in the current prototype |
|---|---|---|
| Kafka broker | `producer.py`, `consumer.py` | `localhost:9092` |
| Kafka topic | `producer.py`, `consumer.py` | `weather_data` |
| Poll interval | `producer.py` | Set `INTERVAL_SECONDS = 300` |
| City | Producer CLI | `Berlin` |
| Ollama model | `consumer.py` | `llama3.2` |
| Ollama API | `consumer.py` | `http://localhost:11434` |
| MongoDB | `consumer.py`, `dashboard.py` | `localhost:27017` |
| Database / collection | Consumer, dashboard | `weather_project` / `weather_analysis` |

The current consumer and producer have several hard-coded settings. Moving all configuration (especially MongoDB credentials) to environment variables is a priority before public deployment or collaboration.

## Stop services

Stop Python processes with `Ctrl+C`. Then stop the containers:

```powershell
podman compose down
```

This preserves MongoDB's named volume. **Warning:** `podman compose down -v` also removes attached volumes and can delete stored weather analyses.

## Troubleshooting

- **`podman` exit code 125:** Check `podman machine list`, try `podman --log-level=debug machine start`, then inspect `wsl --list --verbose` on Windows.
- **Kafka connection refused:** Check `podman compose ps` and `podman logs local-kafka`; the host Python scripts use `localhost:9092`.
- **Ollama unavailable/model missing:** Check `ollama list`, `ollama serve`, and `http://localhost:11434`.
- **MongoDB authentication error:** Check the credentials in Compose and in both Python scripts. MongoDB initialization environment variables only take effect on a fresh database volume.
- **Dashboard shows old data:** Ensure the producer and consumer are both running; the dashboard only reads records already processed and persisted.
- **PowerShell line continuations:** The commands above are deliberately single-line. Bash-style `\` line continuations do not work in PowerShell.

## Roadmap

- [X] Move credentials and connection settings to `.env` / environment variables.
- [X] Add city selection and filtering in the dashboard.
- [X] Add historical weather/sentiment graphs.
- [ ] Add AI-based clothing suggestions for children.
- [ ] Add text field to search for specific town (maybe also PLZ)
- [ ] Add configuration for user: amount of children, gender, maybe tags with activities they like
- [X] Add tests, linting, and CI.
- [ ] Improve resilience for Ollama outages and invalid records.
- [X] Add a license and contribution guidelines.

## Data sources and responsible use

Weather and geocoding data come from [Open-Meteo](https://open-meteo.com/). Follow its applicable usage limits, terms, and attribution requirements. AI recommendations are informational; check current conditions and local safety guidance before taking a child outdoors.

## Development & Quality Assurance

WeatherWatcher uses automated code quality checks to maintain consistent and reliable development standards.

### Development Tools

- **Ruff** — Python linting and code formatting.
- **pytest** — Automated unit testing.
- **GitHub Actions** — Continuous integration for pushes and pull requests.

### Running Quality Checks

Install development dependencies:

```bash
python -m pip install -r requirements-dev.txt
```

Run linting, formatting checks, and tests:

```bash
python -m ruff check .
python -m ruff format --check .
python -m pytest -q
```

These checks also run automatically through GitHub Actions.

### Contributing

Contributions, bug reports, and feature suggestions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) before submitting a pull request.

### License

WeatherWatcher is distributed under the MIT License. See [LICENSE](LICENSE) for details.
