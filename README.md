# openetl_scaffold

Scaffold for Python data pipeline repositories managed by [System2](https://github.com/system2-io/system2).

Clone this repo to bootstrap a new pipeline repository. It provides an orchestrator-agnostic `lib/` of shared utilities and an `example/` pipeline to copy-and-rename for each new data source.

## Quick start

### 1. Clone and detach

```bash
git clone https://github.com/diegoscarabelli/openetl_scaffold.git system2_data_pipelines
cd system2_data_pipelines
git remote remove origin
```

If you have a GitHub account, create your own remote:

```bash
gh repo create system2_data_pipelines --private --source=. --remote=origin --push
```

### 2. Set up credentials

```bash
cp .env.example .env
# Edit .env — it is gitignored. Fill in DB_PASSWORD and other values.
```

### 3. Create database objects (run as Postgres superuser)

```bash
psql -U postgres -f database.ddl       # create database (once)
psql -U postgres -d system2_pipelines_db -f schemas.ddl  # create schemas
```

Create the app user with read/write but no DDL privileges:

```sql
CREATE USER system2_pipelines WITH PASSWORD 'your_password';
GRANT CONNECT ON DATABASE system2_pipelines_db TO system2_pipelines;
-- Repeat after running each pipeline's tables.ddl:
GRANT USAGE ON SCHEMA example TO system2_pipelines;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA example TO system2_pipelines;
ALTER DEFAULT PRIVILEGES IN SCHEMA example
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO system2_pipelines;
```

### 4. Install Python dependencies

```bash
pip install -r requirements.txt
# Also install your orchestrator:
pip install prefect>=3.0          # Prefect
# or: pip install apache-airflow>=3.0
```

### 5. Create a new pipeline

```bash
cp -r pipelines/example pipelines/my_pipeline
```

Then edit:
- `constants.py` — file type regex patterns
- `process.py` — implement `process_file_set()` with domain logic
- `sqla_models.py` — ORM models matching your DB schema
- `tables.ddl` — DDL for your tables
- `dag.py` (Airflow) or `flow.py` (Prefect) — thin entry point

## Airflow 3

Set `dags_folder` in `airflow.cfg` to the repo root — Airflow scans recursively:

```ini
[core]
dags_folder = /absolute/path/to/system2_data_pipelines
```

## Prefect

Run locally:

```bash
python -m pipelines.example.flow
```

Or deploy to a Prefect server/Cloud.

## Data directories

Each pipeline uses four directories under `data/{pipeline_id}/`:

| Directory | Purpose |
|-----------|---------| 
| `ingest/` | Drop raw files here to trigger processing |
| `process/` | Staging area while processing is active |
| `store/` | Archive for successfully processed files |
| `quarantine/` | Isolation for files that caused errors |

`data/` is gitignored. Set `DATA_DIR` in `.env` to use a different base path.
