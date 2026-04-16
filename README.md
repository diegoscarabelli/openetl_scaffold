# openetl_scaffold

Scaffold for Python data pipeline repositories managed by [System2](https://github.com/system2-io/system2).

Clone this repo to bootstrap a new pipeline repository. It provides an orchestrator-agnostic `dags/lib/` of shared utilities and a `dags/pipelines/example/` pipeline to copy-and-rename for each new data source.

## Layout

```
dags/                    # All pipeline code; matches the DAGs folder Astro/Airflow expect
  lib/                   # Shared utilities (orchestrator-agnostic + thin adapters)
    pipeline_config.py
    task_utils.py        # ingest, batch, process_wrapper, store
    processor.py         # Processor ABC
    filesystem_utils.py  # DataState, FileSet, ETLDataDirectories
    sql_utils.py         # make_base, fkey, upsert_model_instances, get_engine
    airflow_utils.py     # create_dag() factory
    prefect_utils.py     # create_flow() factory
    logging_utils.py
  pipelines/
    example/             # Template pipeline — copy and rename
      constants.py       # FileType enum of compiled regex patterns
      process.py         # Processor subclass (domain logic)
      sqla_models.py     # SQLAlchemy ORM models
      tables.ddl         # DDL for this pipeline's tables
      dag.py             # Airflow 3 entry point (~10 lines)
      flow.py            # Prefect entry point (~10 lines)
data/                    # Runtime data (gitignored); override with DATA_DIR env var
requirements.txt
schemas.ddl              # CREATE SCHEMA statements
database.ddl             # CREATE DATABASE (run once)
.env.example
```

The top-level folder is called `dags/` because Astro CLI and native Airflow both hardcode that name as their DAGs folder. Prefect does not care about the directory name; putting flows under `dags/` keeps both orchestrators working with zero configuration overrides. Airflow/Astro automatically adds `dags/` to PYTHONPATH, so `from lib.xxx import yyy` resolves without setup. For Prefect, run with `PYTHONPATH=dags`.

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
psql -U postgres -f database.ddl                            # create database (once)
psql -U postgres -d system2_pipelines_db -f schemas.ddl     # create schemas
```

Create the app user with read/write but no DDL privileges:

```sql
CREATE USER system2_pipelines WITH PASSWORD 'your_password';
GRANT CONNECT ON DATABASE system2_pipelines_db TO system2_pipelines;
-- Repeat per pipeline after running its tables.ddl:
GRANT USAGE ON SCHEMA example TO system2_pipelines;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA example TO system2_pipelines;
ALTER DEFAULT PRIVILEGES IN SCHEMA example
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO system2_pipelines;
```

### 4. Install Python dependencies

```bash
pip install -r requirements.txt
# Plus your orchestrator:
pip install 'prefect>=3.0'          # Prefect
# or: pip install 'apache-airflow>=3.0'
```

### 5. Create a new pipeline

```bash
cp -r dags/pipelines/example dags/pipelines/my_pipeline
```

Then edit:
- `constants.py` — file type regex patterns
- `process.py` — implement `process_file_set()` with domain logic
- `sqla_models.py` — ORM models matching your DB schema
- `tables.ddl` — DDL for your tables
- `dag.py` (Airflow) or `flow.py` (Prefect) — thin entry point

## Airflow 3 (via Astro CLI)

`astro dev init` inside this repo creates `.astro/`, `Dockerfile`, etc. Astro's default DAGs folder is `dags/`, which matches this layout — no override files needed. Delete the placeholder `dags/exampledag.py` that `astro dev init` drops in, since our real DAGs are already under `dags/pipelines/<name>/dag.py`.

## Prefect

Run a flow locally:

```bash
PYTHONPATH=dags python -m pipelines.example.flow
```

Or deploy to a Prefect server/Cloud from the `dags/` directory:

```bash
cd dags
prefect deploy pipelines/example/flow.py:flow --name example-prod --work-pool default
```

## Data directories

Each pipeline uses four directories under `data/{pipeline_id}/`:

| Directory    | Purpose                                              |
|--------------|------------------------------------------------------|
| `ingest/`    | Drop raw files here to trigger processing            |
| `process/`   | Staging area while processing is active              |
| `store/`     | Archive for successfully processed files             |
| `quarantine/`| Isolation for files that caused errors               |

`data/` is gitignored. Set `DATA_DIR` in `.env` to use a different base path.
