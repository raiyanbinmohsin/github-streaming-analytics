# GitHub Streaming Analytics

Real-time analytics pipeline for GitHub repositories using Apache Pulsar, Docker Compose, and Python consumers.

## What This Project Answers

- Q1: Top languages by repository count
- Q2: Top repositories by commit count
- Q3: Top languages with unit test folder indicators
- Q4: Top languages that show both unit-test and CI/CD indicators

## Distributed Cloud Architecture (OpenStack)

While this project can be run locally on a single machine, for this assignment, it was deployed as a fully distributed system across 5 isolated OpenStack Virtual Machines to ensure high availability and true decoupled processing.

The infrastructure is divided as follows:
* **VM 1 (Master Node):** Hosts the Apache Pulsar broker (`pulsar://<Master_IP>:6650`).
* **VM 2 (Ingestion Node):** Hosts the `producer` container, managing API rate limits and publishing to the Pulsar topic over the internal cloud network.
* **VM 3 (Analytics Node A):** Hosts `consumer-language-stats`.
* **VM 4 (Analytics Node B):** Hosts `consumer-unit-test-analysis`.
* **VM 5 (Analytics Node C):** Hosts `consumer-ci-devops-analysis`.

**Deployment Strategy:** The workload was distributed using isolated Docker Compose configurations pointing to the Master node. The consumers on the worker nodes asynchronously process messages from the Master node's Pulsar topic. Results were aggregated from the worker nodes via `scp` to generate the final PNG visualizations.

## Current Pipeline

```text
GitHub Search API (day-by-day crawl)
        -> main.py --service producer
        -> Pulsar topic: persistent://public/default/raw-repositories
        -> main.py --service consumer-language-stats
        -> main.py --service consumer-unit-test-analysis
        -> main.py --service consumer-ci-devops-analysis
        -> results/*.json
        -> results/visualize.py -> results/*.png
```

## How It Works

The system runs as a streaming pipeline with one producer and three independent consumers.

1. Producer stage
- `main.py --service producer` runs `producer/github_crawler.py`.
- It queries GitHub Search by date (`pushed:YYYY-MM-DD`) and paginates up to 10 pages per day.
- For each repository, it filters required fields and publishes JSON messages to:
- `persistent://public/default/raw-repositories`

2. Streaming stage (Apache Pulsar)
- Pulsar acts as the event backbone between ingestion and analytics.
- Producer and consumers are decoupled, so consumers can process in parallel.

3. Consumer stage
- `main.py --service consumer-language-stats` computes Q1 and Q2.
- `main.py --service consumer-unit-test-analysis` computes Q3.
- `main.py --service consumer-ci-devops-analysis` computes Q4.
- Each consumer has its own subscription on the same raw topic.

4. Output stage
- Consumers write rolling JSON outputs every 10 processed messages into `results/`.
- `results/visualize.py` reads JSON outputs and generates PNG charts.

5. Orchestration stage (Docker Compose)
- `docker-compose.yml` starts Pulsar plus all Python services.
- Python services share one base config via YAML anchors and only differ by command.
- Services use `restart: unless-stopped` for better resilience.

## Prerequisites

- Docker Engine + Docker Compose
- GitHub Personal Access Token
- Optional local Python 3.11+ (only needed if you run `results/visualize.py` outside Docker)

## GitHub Token Setup

This project uses the GitHub REST API, so you should create a GitHub Personal Access Token (PAT) before running the producer.

1. Open GitHub and go to:
`Settings` -> `Developer settings` -> `Personal access tokens` -> `Tokens (classic)`

2. Click `Generate new token (classic)`.

3. Give the token a clear name, for example `github-streaming-analytics`.

4. Select the minimum scope needed for this project:
`public_repo` for searching and reading public repositories.

5. Click `Generate token`.

6. Copy the token immediately.
GitHub will not show it again after you leave the page.

7. Add it to the project.

If you have not created your local env file yet:

```bash
cp .env_example .env
```

Then open `.env` and set:

```env
GITHUB_TOKEN=your_generated_token_here
```

The app reads `GITHUB_TOKEN` automatically when you run `docker compose up --build` or any `main.py --service ...` command.

## How To Run

### Option 1: Docker Compose (recommended)

1. Clone and enter the project.

```bash
git clone <your-repo-url>
cd github-streaming-analytics
```

2. Configure environment variables.

```bash
cp .env_example .env
```

Edit `.env` and add your GitHub token plus the settings you want to use:

```env
GITHUB_TOKEN=your_token_here
SEARCH_START_DATE=2023-01-01
SEARCH_END_DATE=2023-01-07
TOP_N=10
LOG_LEVEL=INFO
PULSAR_SERVICE_URL=pulsar://pulsar-broker:6650
```

3. Build and start the full stack.

```bash
docker compose up --build
```

4. Stop when done.

```bash
docker compose down
```

### Option 2: Run services manually with main.py

Use this when you want to run a specific service outside Docker Compose.

1. Install dependencies.

```bash
pip3 install -r requirements.txt
```

2. Ensure Apache Pulsar is available and your `.env` is configured.

3. Run services in separate terminals.

```bash
# producer
python3 main.py --service producer

# consumer for Q1 and Q2
python3 main.py --service consumer-language-stats

# consumer for Q3
python3 main.py --service consumer-unit-test-analysis

# consumer for Q4
python3 main.py --service consumer-ci-devops-analysis
```

4. Generate plots (optional).

```bash
python3 results/visualize.py
```

### Verify containers are healthy

```bash
docker compose ps
docker compose logs -f producer
```

## Configuration Files

- `.env` stores runtime settings used by producer and consumers.
- `requirements.txt` defines Python dependencies for local runs and container builds.
- `Dockerfile` builds a shared Python image used by all app services.
- `docker-compose.yml` orchestrates Pulsar + app services with a shared base service definition.
- `pulsar/topic_config.yaml` describes topic defaults and per-topic retention settings.

Topic retention in `pulsar/topic_config.yaml`:

- `raw-repositories`: 1440 minutes, 1024 MB
- `language-stats`: 1440 minutes, 512 MB
- `unit-test-analysis`: 1440 minutes, 512 MB
- `ci-devops-analysis`: 1440 minutes, 512 MB

## Environment Variables

| Variable | Purpose | Default |
|---|---|---|
| `GITHUB_TOKEN` | Auth token for higher GitHub API limits | empty |
| `SEARCH_START_DATE` | Crawl start date (`YYYY-MM-DD`) | `2023-01-01` |
| `SEARCH_END_DATE` | Crawl end date (`YYYY-MM-DD`) | `2023-01-01` |
| `TOP_N` | Number of entries kept in results | `10` |
| `LOG_LEVEL` | Application log verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`) | `INFO` |
| `PULSAR_SERVICE_URL` | Pulsar broker URL | `pulsar://pulsar-broker:6650` |

## Logging

All services use Python logging with a shared format:

- `%(asctime)s %(levelname)s [%(name)s] %(message)s`

By default, logs run at `INFO`. Change verbosity using `LOG_LEVEL` in `.env`.

Examples:

- `LOG_LEVEL=DEBUG` for detailed troubleshooting
- `LOG_LEVEL=INFO` for normal runs
- `LOG_LEVEL=WARNING` to reduce noise

To inspect logs while running with Docker Compose:

```bash
docker compose logs -f producer
docker compose logs -f consumer-language-stats
docker compose logs -f consumer-unit-test-analysis
docker compose logs -f consumer-ci-devops-analysis
```

## Recommended Settings (First Run)

Use a short date window first so you can validate the full pipeline quickly.

```env
GITHUB_TOKEN=your_token_here
SEARCH_START_DATE=2023-01-01
SEARCH_END_DATE=2023-01-03
TOP_N=10
LOG_LEVEL=INFO
PULSAR_SERVICE_URL=pulsar://pulsar-broker:6650
```

Recommendations:

- Always set `GITHUB_TOKEN` to reduce GitHub API throttling.
- Start with 1-3 days of data, then increase the date range after validating outputs.
- Keep `PULSAR_SERVICE_URL` as `pulsar://pulsar-broker:6650` when running with Docker Compose.
- Keep `TOP_N=10` initially; increase later if you need wider rankings.

## Outputs

Consumers continuously write:

- `results/language_stats.json` (Q1 and Q2)
- `results/unit_test_analysis.json` (Q3)
- `results/ci_devops_analysis.json` (Q4)

Generate charts from those JSON files:

```bash
python results/visualize.py
```

Expected chart files:

- `results/q1_languages.png`
- `results/q2_commits.png`
- `results/q3_tdd.png`
- `results/q4_ci_devops.png`

If running visualization locally for the first time:

```bash
pip3 install matplotlib
```

## Troubleshooting

### Docker Compose warning: `the attribute version is obsolete`

This warning is fixed in the current [docker-compose.yml](docker-compose.yml).
If you still see it, make sure you are running the latest file version from your current branch.

### `IndentationError` in `results/visualize.py`

This is fixed in the current [results/visualize.py](results/visualize.py).
You can validate syntax with:

```bash
python3 -m py_compile results/visualize.py
```

## Services (Docker Compose)

- `pulsar`: Apache Pulsar standalone broker
- `producer`: runs `main.py --service producer`
- `consumer-language-stats`: runs `main.py --service consumer-language-stats`
- `consumer-unit-test-analysis`: runs `main.py --service consumer-unit-test-analysis`
- `consumer-ci-devops-analysis`: runs `main.py --service consumer-ci-devops-analysis`

All consumers currently subscribe to the same topic:

- `persistent://public/default/raw-repositories`

## Project Structure

```text
.
├── Dockerfile
├── docker-compose.yml
├── .env_example
├── main.py
├── producer/
│   ├── github_crawler.py
│   └── rate_limiter.py
├── consumers/
│   ├── language_stats/consumer.py
│   ├── unit_test_analysis/consumer.py
│   └── ci_devops_analysis/consumer.py
├── pulsar/topic_config.yaml
├── results/
│   ├── language_stats.json
│   ├── unit_test_analysis.json
│   ├── ci_devops_analysis.json
│   └── visualize.py
└── report/
```

## Notes and Limitations

- GitHub Search is queried by `pushed:<date>` and paginated up to 10 pages/day.
- Commit counts and repository content checks trigger extra GitHub API calls and may hit rate limits on large date ranges.
- Results are periodically updated while consumers run (every 10 processed messages).

## References

- GitHub Search API: https://docs.github.com/en/rest/search/search#search-repositories
- GitHub rate limits: https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api
- Apache Pulsar docs: https://pulsar.apache.org/docs/
