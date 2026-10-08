# Real Madrid C.F. Data Lakehouse

An end-to-end modern Data Lakehouse engineered to ingest, process, conform, and serve match and player performance analytics for Real Madrid C.F. Built with PySpark, Delta Lake, dbt, and Apache Airflow, this repository implements a decoupled, stage-based medallion architecture replacing traditional, rigid relational ETL pipelines.

## Table of Contents

- [Project Overview](#project-overview)
- [System Architecture](#system-architecture)
- [Medallion Storage Stages](#medallion-storage-stages)
- [Repository Layout](#repository-layout)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Local Installation](#local-installation)
- [CI/CD and Code Quality](#cicd-and-code-quality)
- [Data Modeling and Metrics](#data-modeling-and-metrics)
- [Roadmap](#roadmap)
- [License](#license)

---

## Project Overview

Tracking modern football analytics at club scale requires processing high-volume event streams (progressive actions, expected metrics, defensive recoveries, and positional coordinates). Traditional relational databases struggle with schema fluctuations and lack table versioning, while flat data lakes lack ACID transactions.

This platform bridges both paradigms:
- **Decoupled Architecture:** Scalable columnar Delta tables on local or cloud object storage.
- **ACID Guarantees and Time Travel:** Atomic `MERGE INTO` operations prevent duplicate match logs on retries.
- **Tailored Scoping:** Conforms match logs specifically scoped to Real Madrid fixtures across La Liga and UEFA Champions League competitions.

---

## System Architecture

The pipeline automates data movement from external football data providers through a three-stage Lakehouse model to serving endpoints.

<p align="center">
  <img src="docs/architecture_diagram.png" alt="Real Madrid C.F. Data Lakehouse Architecture" width="800"/>
</p>

- **Orchestrator (Airflow):** Manages job triggers, API quotas, dependency trees, and backoff retries.
- **Ingestion Engine (PySpark ETL):** Extracts payloads from external REST endpoints, stamps ingestion metadata, and writes immutable Delta snapshots.
- **Storage Engine (Medallion Lakehouse):** Organizes storage into Stage 1, Stage 2, and Stage 3 with strict schema validation.
- **Transformations (dbt):** Executes in-lake SQL transformations to build star-schema models and pre-computed tactical aggregates.
- **Serving and BI (PostgreSQL / Dashboards):** Exposes optimized performance tables to tactical dashboards and machine learning models.

---

## Medallion Storage Stages

| Stage | Target Path | Format | Description and Scope |
| :--- | :--- | :--- | :--- |
| **Stage 1 (Raw)** | `data/stage1/matches` | Delta / Parquet | **Immutable Landing Zone:** Unaltered match and player event JSON/CSV dumps appended with `_ingested_at` and `_source_uri`. |
| **Stage 2 (Cleaned)** | `data/stage2/stats` | Delta Lake | **Conformed Layer:** Filtered specifically for Real Madrid matches (`home_team == 'Real Madrid'` or `away_team == 'Real Madrid'`), typed columns, null reconciliation, and deduplicated keys. |
| **Stage 3 (Aggregations)** | `data/stage3/performance` | Delta Lake | **Analytical Serving:** Star-schema models featuring per-90 metrics, expected goals differential ($xG - xGA$), progressive carry tracking, and rolling squad trends. |

---

## Repository Layout

```text
football_r1/
├── .github/
│   └── workflows/
│       ├── ci.yml               # Linting, style checks and PySpark test suite
│       └── docker-build.yml     # Docker build smoke test
├── data/                        # Lakehouse Storage
│   ├── stage1/                  # Raw landing tables (matches, player logs)
│   ├── stage2/                  # Cleaned and conformed dimensional tables
│   └── stage3/                  # Aggregated metrics and tactical tables
├── docker/
│   ├── Dockerfile.spark         # Containerized Spark runtime
│   └── docker-compose.yml       # Local orchestration stack
├── docs/
│   └── architecture_diagram.png # Architecture diagram image
├── src/
│   ├── __init__.py
│   ├── ingestion/               # API extraction scripts and clients
│   ├── transformation/          # Stage 1 -> Stage 2 -> Stage 3 ETL logic
│   └── utils/                   # SparkSession helpers and Delta configurations
├── tests/
│   ├── __init__.py
│   ├── integration/             # Pipeline execution tests
│   └── unit/                    # Metric computations and schema checks
├── pyproject.toml               # Ruff and Pytest configuration
├── requirements.txt             # Core runtime dependencies
├── requirements-dev.txt         # Testing and linting tools
└── README.md
```

---

## Tech Stack

| Domain | Technology | Purpose |
| :--- | :--- | :--- |
| **Distributed Compute** | Apache Spark (PySpark 3.5) | Scalable distributed processing engine |
| **Lake Storage Format** | Delta Lake 3.2 | ACID transactions, unified batch processing, time-travel |
| **Orchestration** | Apache Airflow | Directed Acyclic Graph (DAG) scheduling and workflow monitoring |
| **Transformations** | dbt (Data Build Tool) | Modular SQL transformations, testing, and documentation |
| **Serving and Storage** | PostgreSQL / BI Connectors | Relational serving layer for dashboards and tactical tooling |
| **Code Quality** | Ruff and Pytest | High-performance linting and automated pipeline unit tests |
| **CI/CD** | GitHub Actions | Automated linting, test runners, and container verification |

---

## Getting Started

### Prerequisites

- **Python**: 3.10 or 3.11
- **Java**: OpenJDK 17 (Required for running PySpark execution engines locally)
- **Docker and Docker Compose** (Optional, for containerized execution)

### Local Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/<your-username>/football_r1.git
   cd football_r1
   ```

2. Create and activate a virtual environment:
   - **Windows (PowerShell):**
     ```powershell
     python -m venv .venv
     .venv\Scripts\Activate.ps1
     ```
   - **Linux / macOS:**
     ```bash
     python3 -m venv .venv
     source .venv/bin/activate
     ```

3. Install dependencies:
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   pip install -r requirements-dev.txt
   ```

4. Add the architecture diagram: Place your architecture diagram image into the docs folder:
   ```text
   docs/architecture_diagram.png
   ```

---

## CI/CD and Code Quality

This repository enforces strict code style, typing, and test execution on every pull request via GitHub Actions.

### Run Code Linter and Formatter

```bash
# Check for lint errors
ruff check .

# Check formatting
ruff format --check .

# Auto-fix formatting
ruff format .
```

### Run Test Suite

```bash
pytest tests/ -v
```

---

## Data Modeling and Metrics

### Analytical Equations Computed in Stage 3

- **Normalized Metric per 90 Minutes:**
  $$\text{Metric}_{90} = \left( \frac{\text{Total Event Count}}{\text{Minutes Played}} \right) \times 90$$

- **Expected Goals Difference ($\Delta xG$):**
  $$\Delta xG = xG_{\text{Created}} - xG_{\text{Conceded}}$$

- **Field Tilt Percentage:**
  $$\text{Field Tilt} = \left( \frac{\text{Final Third Passes}_{\text{Real Madrid}}}{\text{Final Third Passes}_{\text{Total}}} \right) \times 100$$

---

## Roadmap

- [x] Initial repository structure and CI/CD workflow configuration
- [x] Stage-based lakehouse storage schema definition
- [ ] PySpark API ingestion client with retry and rate-limiting
- [ ] Stage 1 to Stage 2 PySpark cleaning and Real Madrid filter logic
- [ ] Stage 2 to Stage 3 dbt transformation models
- [ ] Airflow DAG configuration for match-day automated runs
- [ ] PowerBI / Streamlit dashboard serving integration

---

## License

Distributed under the MIT License. See `LICENSE` for more information.
