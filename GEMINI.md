# Enedis Energy Intelligence Pipeline

A comprehensive ELT (Extract, Load, Transform) and Analytics platform for Enedis (French electricity distributor) half-hourly energy balance data.

## Project Overview

This project implements a complete data lifecycle from ingestion to visualization, following a Medallion architecture (Bronze, Silver, Gold layers).

### Architecture & Technologies

- **Orchestration**: [Apache Airflow](https://airflow.apache.org/) running in Docker.
- **Data Ingestion**: Custom Python scripts (`fetch_enedis.py`, `fetch_price.py`) fetching data from Open Data APIs.
- **Data Warehouse**: [PostgreSQL](https://www.postgresql.org/).
- **Transformations**: [dbt](https://www.getdbt.com/) for building the data layers (Bronze, Silver, Gold).
- **Data Analytics**: Python-based analysis using [Polars](https://pola.rs/), [XGBoost](https://xgboost.readthedocs.io/), [LightGBM](https://lightgbm.readthedocs.io/), and [statsmodels](https://www.statsmodels.org/).
- **Visualization**: [Plotly Dash](https://dash.plotly.com/) for interactive dashboards.
- **Environment Management**: `docker-compose` for the infrastructure, `uv` for Python dependencies.

## Directory Structure

- `data_pipeline/`: Airflow DAGs, plugins, and dbt project.
  - `airflow/dags/`: ELT and Price fetching DAGs.
  - `airflow/plugins/`: Core logic for API interaction and database loading.
  - `dbt/`: dbt models for data transformation.
- `data_analytics/`: Data science workspace.
  - `notebooks/`: Jupyter notebooks for EDA, modeling, and anomaly detection.
  - `scripts/`: Production-ready scripts for data processing.
  - `src/`: Shared logic for modeling and visualization.
- `data_dashboard/`: Web application for visualizing the energy insights.
- `docker-compose.yaml`: Orchestrates the entire stack (Postgres, Airflow, pgAdmin, Dashboard).

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python 3.12+ (for local development)
- `uv` (recommended for Python environment management)

### Running the Stack

1. **Environment Setup**:
   Copy `env.example` to `.env` and fill in the required variables (API keys, DB credentials).
   ```bash
   cp env.example .env
   ```

2. **Start Services**:
   ```bash
   docker compose up -d
   ```

3. **Access Interfaces**:
   - **Airflow**: [http://localhost:8080](http://localhost:8080) (Default login: `admin`/`admin`)
   - **Dashboard**: [http://localhost:8050](http://localhost:8050)
   - **pgAdmin**: [http://localhost:5050](http://localhost:5050) (Login via `PGADMIN_EMAIL` in `.env`)

### Data Analytics Workflow

For local analysis in `data_analytics/`:
1. Install dependencies:
   ```bash
   cd data_analytics
   uv sync
   ```
2. Run anomaly detection:
   ```bash
   python scripts/decompose_and_detect_anomalies.py
   ```

## Development Conventions

- **Python**: Use Python 3.12. Linting and formatting are handled by `ruff`.
- **dbt**: Models should follow the Medallion naming convention (`bronze_*`, `silver_*`, `gold_*`).
- **Data Loading**: Ingestion scripts use `PostgresHook` and `httpx` for efficient, asynchronous fetching.
- **Modeling**: Prefer `Polars` for data manipulation over `Pandas` for performance.
- **Tests**: Data validation tests are located in `data_analytics/tests/`.

## Key Commands

- `make data`: (In `data_analytics/`) Processes raw data.
- `make train`: (In `data_analytics/`) Trains the predictive models.
- `dbt run`: Executes dbt transformations (usually triggered by Airflow).
- `pytest`: Runs the test suite.
