# openetl_scaffold

Contents:

- [Introduction](#introduction)
- [Getting Started](#getting-started)
- [Repository Structure](#repository-structure)
- [Standard Pipeline](#standard-pipeline)
- [Running Pipelines](#running-pipelines)
- [Database Environments](#database-environments)
- [Data Directories](#data-directories)
- [Contributing](#contributing)

## Introduction

OpenETL Scaffold is a starter template for building Python ETL data pipelines with [PostgreSQL](https://www.postgresql.org/) as the analytics database. It provides orchestrator-agnostic shared utilities and thin adapters for both [Apache Airflow](https://airflow.apache.org/) and [Prefect](https://www.prefect.io/), so the same pipeline logic runs under either orchestrator with minimal wiring.

Unlike [OpenETL](https://github.com/diegoscarabelli/openetl), which is tightly integrated with a specific data stack (TimescaleDB, Airflow-only orchestration, credential-file auth, IAM with per-pipeline users), this scaffold is intentionally lightweight. The only infrastructure requirement is a PostgreSQL database. Clone, rename, and build on top of it.

Key characteristics:

- **Orchestrator-agnostic core**: The shared library (`dags/lib/`) implements the four-step ETL pattern (ingest, batch, process, store) without importing any orchestrator. Airflow and Prefect adapters are thin wrappers.
- **Single PostgreSQL dependency**: No TimescaleDB, PostGIS, or vectorscale extensions required (though you can add them). Database connection is configured via environment variables.
- **Dual orchestrator support**: Each pipeline includes both a `dag.py` (Airflow 3) and a `flow.py` (Prefect 3) entry point, sharing all processing logic.
- **Example pipeline included**: A working WID.world (World Inequality Database) pipeline demonstrates the full pattern with API extraction, incremental loading, dimension seeding, and fact table upserts.

## Getting Started

### Prerequisites

- **PostgreSQL 14+** installed and running. Version 14 is the minimum because the IAM script uses `pg_read_all_data` and `pg_write_all_data` predefined roles.
- **Python 3.10+** with pip.
- **One orchestrator** (optional for local testing): `apache-airflow>=3.0` or `prefect>=3.0`.

### Clone and Detach

```bash
git clone https://github.com/diegoscarabelli/openetl_scaffold.git my_data_pipelines
cd my_data_pipelines
git remote remove origin
```

If you have a GitHub account, create your own remote:

```bash
gh repo create my_data_pipelines --private --source=. --remote=origin --push
```

### Database Installation and Initialization

#### Set up credentials

```bash
cp .env.template .env
# Edit .env — it is gitignored. Fill in SQL_DB_PASSWORD and other values.
```

The `.env` file configures the database connection used by all pipelines at runtime. Key variables:

| Variable | Purpose | Default |
|----------|---------|---------|
| `SQL_DB_HOST` | Database hostname. | `localhost` |
| `SQL_DB_PORT` | Database port. | `5432` |
| `SQL_DB_NAME` | Database name. | (from `database.ddl`) |
| `SQL_DB_USER` | Pipeline service user. | (from `iam.sql`) |
| `SQL_DB_PASSWORD` | Password for the service user. | (none) |
| `DATABASE_URL` | Full SQLAlchemy URL (overrides all above). | (none) |
| `DATA_DIR` | Pipeline file storage directory. | `./data` |

The `SQL_DB_*` prefix (not `DB_*`) avoids conflicts with Airflow's entrypoint script, which reserves `DB_HOST` for health-check logic.

#### Initialize the database

Edit `iam.sql` first to replace the two `<REDACTED>` password placeholders with real passwords, then run:

```bash
# Step 1: Create the databases (production + development).
psql -U postgres -d postgres -f database.ddl

# Step 2: Initialize schemas and extensions (run against each database you use).
psql -U postgres -d <your_db> -f schemas.ddl

# Step 3: Create roles, users, and permissions.
psql -U postgres -d <your_db> -f iam.sql

# Step 4: Create pipeline tables.
psql -U postgres -d <your_db> -f dags/pipelines/wid/tables.ddl
```

> **Note:** If TimescaleDB is not installed, comment out the `CREATE EXTENSION` and matching `COMMENT ON EXTENSION` statements in `schemas.ddl` before running Step 2.
>
> **Tip:** If your PostgreSQL `pg_hba.conf` requires password authentication, connect using `psql -h localhost` to trigger password prompts.

`iam.sql` creates:

- **`readers` role**: read-only permission bundle (via `pg_read_all_data`).
- **`read_only` user**: login for BI tools, notebooks, ad-hoc SQL; granted `readers`.
- **Pipeline service user**: login for pipeline read+write; granted `pg_read_all_data` + `pg_write_all_data`.

Grants are blanket across all non-system schemas (including future ones), so no further IAM changes are needed when adding a new pipeline schema.

### Install Python Dependencies

```bash
pip install -r requirements.txt

# Plus your orchestrator:
pip install 'prefect>=3.0'            # Prefect
# or: pip install 'apache-airflow>=3.0'  # Airflow
```

### Development Environment Setup

Install the development dependencies for testing, linting, and formatting:

```bash
pip install -r requirements_dev.txt
```

Set up pre-commit hooks:

```bash
pre-commit install
```

Run the test suite to verify your setup:

```bash
make test
```

Tests connect to a PostgreSQL instance on `localhost:5432` using the `postgres` database with throwaway tables. No pipeline schemas are needed for testing. Override the connection via `TEST_DB_URL` in your `.env` file if needed.

## Repository Structure

### [dags/lib/](dags/lib)

Shared utilities that are orchestrator-agnostic, plus thin adapter modules for Airflow and Prefect. Pipelines import from `lib.*` and never reference orchestrator APIs directly (except in `dag.py` and `flow.py` entry points).

- **[`etl_config.py`](dags/lib/etl_config.py)**: `ETLConfig` base dataclass providing unified configuration for pipeline parameters, file processing settings, database connections, and directory management with validation and sensible defaults.

- **[`task_utils.py`](dags/lib/task_utils.py)**: Orchestrator-agnostic implementations of the four standard tasks (`ingest`, `batch`, `process_wrapper`, `store`) and the abstract `Processor` base class. This is where the ETL pattern lives. Each task function accepts an `ETLConfig` and operates on files and database sessions without importing Airflow or Prefect.

- **[`filesystem_utils.py`](dags/lib/filesystem_utils.py)**: `DataState` enum for pipeline data states (ingest, process, store, quarantine), `ETLDataDirectories` for standardized directory management, and `FileSet` for coordinating file processing across different file types with JSON serialization support.

- **[`sql_utils.py`](dags/lib/sql_utils.py)**: Database utilities including `get_engine()` for creating engines from environment variables, `make_base()` for schema-scoped declarative bases with optional timestamp columns, `fkey()` for fully-qualified foreign key references, and `upsert_model_instances()` for bulk `INSERT ... ON CONFLICT` operations.

- **[`airflow_utils.py`](dags/lib/airflow_utils.py)**: Thin Airflow adapter. `AirflowETLConfig` extends `ETLConfig` with DAG-specific parameters (schedule, timeouts, retries, callbacks). `create_dag()` factory assembles the standard four-task DAG with dynamic task mapping for parallel processing.

- **[`prefect_utils.py`](dags/lib/prefect_utils.py)**: Thin Prefect adapter. `PrefectETLConfig` extends `ETLConfig` with flow-specific parameters (retries, timeouts, tags). `create_flow()` factory assembles the standard four-task flow with `.map()` for parallel processing.

- **[`logging_utils.py`](dags/lib/logging_utils.py)**: Logging utilities that work in both orchestrator and standalone contexts.

### [dags/pipelines/](dags/pipelines)

Contains a subdirectory for each pipeline. Each subdirectory includes:

- `constants.py` — `FileType` enum of compiled regex patterns and pipeline-specific configuration.
- `extract.py` — Data extraction logic (API calls, file downloads, etc.).
- `process.py` — `Processor` subclass with domain logic for parsing files, building ORM instances, and upserting to the database.
- `sqla_models.py` — SQLAlchemy ORM models matching the DDL.
- `tables.ddl` — SQL definitions for this pipeline's tables, indexes, and comments.
- `dag.py` — Airflow 3 entry point.
- `flow.py` — Prefect 3 entry point.

The included `wid/` pipeline (World Inequality Database) is a working example. Copy and rename it to create new pipelines:

```bash
cp -r dags/pipelines/wid dags/pipelines/my_pipeline
```

### [database.ddl](database.ddl)

Creates the production and development databases. Must be executed first, before any schema or table initialization. Edit the database names to match your project.

### [schemas.ddl](schemas.ddl)

Creates PostgreSQL extensions and per-pipeline schemas. Add one `CREATE SCHEMA` block per pipeline. If TimescaleDB or other extensions are not needed, comment out the corresponding `CREATE EXTENSION` and `COMMENT ON EXTENSION` statements.

### [iam.sql](iam.sql)

Identity and access management configuration. Creates a read-only role/user for data consumers and a pipeline service user with read+write access. Uses PostgreSQL 14+ predefined roles (`pg_read_all_data`, `pg_write_all_data`) for blanket permissions across all schemas. Replace the `<REDACTED>` password placeholders before running.

### [tests/](tests)

[Pytest](https://docs.pytest.org/en/stable/)-based test suite. Tests use a local PostgreSQL instance with throwaway tables in the `postgres` database, independent of pipeline schemas. Run with:

```bash
make test
```

## Standard Pipeline

### Pipeline Sequence

Every pipeline follows a five-step sequence. The first step (extract) is pipeline-specific; the remaining four are provided by the shared library:

1. **Extract**: Fetch data from an external source (API, file download, etc.) and save raw files to the `ingest/` directory. Implemented per-pipeline in `extract.py`.
2. **Ingest**: Route files from `ingest/` to `process/` or `store/` based on regex patterns in the configuration. Raises `RuntimeError` when no files are found.
3. **Batch**: Group files by timestamp into `FileSet` objects, then distribute them across batches respecting `max_process_tasks` and `min_file_sets_in_batch` configuration.
4. **Process**: Run the pipeline's `Processor` subclass on each batch. Each `FileSet` gets an independent database transaction. Failures are captured per-FileSet without affecting others. Batches run in parallel (Airflow dynamic task mapping / Prefect `.map()`).
5. **Store**: Move successfully processed files to `store/` and failed files to `quarantine/`.

### Example: Airflow DAG

```python
from datetime import timedelta

from airflow.providers.standard.operators.python import PythonOperator

from lib.airflow_utils import AirflowETLConfig, create_dag

from .constants import MyFileTypes
from .extract import extract
from .process import MyProcessor

config = AirflowETLConfig(
    pipeline_id="my_pipeline",
    pipeline_print_name="My Pipeline",
    description="Description of what this pipeline does.",
    file_types=MyFileTypes,
    processor_class=MyProcessor,
    dag_schedule_interval=None,
    process_format=r".*\.json$",
    db_schema="my_schema",
)

# Create the standard four-task DAG.
dag = create_dag(config)

# Add an extract task before ingest.
with dag:
    task_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
        op_kwargs={"ingest_dir": config.data_dirs.ingest},
    )
    task_extract >> dag.get_task("ingest")
```

This produces: `extract >> ingest >> batch >> process >> store`.

### Example: Prefect Flow

```python
from lib.task_utils import batch, ingest, process_wrapper, store

from .constants import MyFileTypes
from .extract import extract as extract_data
from .process import MyProcessor


def _build_flow():
    from prefect import flow, task
    from prefect.cache_policies import NONE

    from lib.prefect_utils import PrefectETLConfig

    config = PrefectETLConfig(
        pipeline_id="my_pipeline",
        pipeline_print_name="My Pipeline",
        description="Description of what this pipeline does.",
        file_types=MyFileTypes,
        processor_class=MyProcessor,
        process_format=r".*\.json$",
        db_schema="my_schema",
    )

    @task(cache_policy=NONE, name="my_pipeline.extract")
    def _extract():
        extract_data(ingest_dir=config.data_dirs.ingest)

    @task(cache_policy=NONE, name="my_pipeline.ingest")
    def _ingest():
        try:
            return ingest(config)
        except RuntimeError:
            return 0

    @task(cache_policy=NONE, name="my_pipeline.batch")
    def _batch():
        return batch(config)

    @task(cache_policy=NONE, name="my_pipeline.process")
    def _process(serialized_file_sets):
        return process_wrapper(serialized_file_sets, config)

    @task(cache_policy=NONE, name="my_pipeline.store")
    def _store(all_results):
        return store(all_results, config)

    @flow(name="my_pipeline", description=config.description)
    def pipeline_flow():
        _extract()
        count = _ingest()
        if not count:
            return
        batches = _batch()
        futures = _process.map(batches)
        all_results = [f.result(raise_on_failure=False) for f in futures]
        _store(all_results)

    return pipeline_flow


flow = _build_flow()

if __name__ == "__main__":
    flow()
```

## Running Pipelines

### Airflow 3

#### Via Astro CLI

`astro dev init` inside this repo creates `.astro/`, `Dockerfile`, etc. Astro's default DAGs folder is `dags/`, which matches this layout with no overrides needed. Delete the placeholder `dags/exampledag.py` that `astro dev init` drops in, since real DAGs are under `dags/pipelines/<name>/dag.py`.

#### Why `dags/`?

The top-level folder is called `dags/` because Astro CLI and native Airflow both hardcode that name as their DAGs folder. Airflow/Astro automatically adds `dags/` to `PYTHONPATH`, so `from lib.xxx import yyy` resolves without setup.

### Prefect

Prefect does not care about the directory name. Putting flows under `dags/` keeps both orchestrators working with zero configuration overrides.

Run a flow locally:

```bash
PYTHONPATH=dags python -m pipelines.wid.flow
```

Deploy to a Prefect server or Cloud from the `dags/` directory:

```bash
cd dags
prefect deploy pipelines/wid/flow.py:flow --name wid-prod --work-pool default
```

## Database Environments

`database.ddl` creates two databases: one for production and one for development. Both share the same schemas, tables, and IAM configuration. Run `schemas.ddl`, `iam.sql`, and your pipeline DDL scripts against each database you use.

To switch between them, change `SQL_DB_NAME` in your `.env` file (or set `DATABASE_URL`):

- **Pipeline development** (running DAGs/flows locally): point `SQL_DB_NAME` at the development database.
- **Production** (deployed orchestrator): point `SQL_DB_NAME` at the production database.
- **Unit tests** (pytest): tests use the default `postgres` database with throwaway tables, configured via `TEST_DB_URL` in `conftest.py`. No pipeline schemas needed.

## Data Directories

Each pipeline uses four directories under `data/{pipeline_id}/`:

| Directory    | Purpose                                              |
|--------------|------------------------------------------------------|
| `ingest/`    | Drop raw files here to trigger processing.           |
| `process/`   | Staging area while processing is active.             |
| `store/`     | Archive for successfully processed files.            |
| `quarantine/`| Isolation for files that caused errors.              |

`data/` is gitignored. Set `DATA_DIR` in `.env` to use a different base path. For Astronomer/Docker setups, use an absolute host-side path (Docker Compose does not expand `~`).

## Contributing

For detailed contribution guidelines, see [CONTRIBUTING.md](CONTRIBUTING.md), which covers:

- **External contributors workflow**: fork-based contribution process.
- **Code standards**: Python formatting (Black, Autoflake, Docformatter), SQL formatting (SQLFluff), pre-commit hooks.
- **Testing**: requirements, coverage expectations, and how to run tests.
- **Pull request process**: pre-submission checklist and code review expectations.

### Quick Start for Contributors

1. Complete the [Getting Started](#getting-started) setup.
2. Fork the repository and clone your fork.
3. Create a feature branch: `git checkout -b feature/your-feature`.
4. Make your changes following the [code standards](CONTRIBUTING.md#formatting).
5. Run tests: `make test`.
6. Format code: `make format && make check-format`.
7. Commit, push, and open a Pull Request.
