# openetl_scaffold

Contents:

- [Introduction](#introduction)
- [Getting Started](#getting-started)
- [Running Pipelines](#running-pipelines)
- [Database Environments](#database-environments)
- [Standard Pipeline](#standard-pipeline)
- [Data Directories](#data-directories)
- [Repository Structure](#repository-structure)
- [Contributing](#contributing)

## Introduction

OpenETL Scaffold is a starter template for building Python ETL data pipelines with [PostgreSQL](https://www.postgresql.org/) as the analytics database. It provides orchestrator-agnostic shared utilities and thin adapters for both [Apache Airflow](https://airflow.apache.org/) and [Prefect](https://www.prefect.io/), so the same pipeline logic runs under either orchestrator with minimal wiring.

OpenETL Scaffold is extracted from [OpenETL](https://github.com/diegoscarabelli/openetl), stripped down to the core ETL framework. The only infrastructure requirement is a PostgreSQL database. Clone it and detach from this upstream to start your own repository of data pipelines.

Key characteristics:

- **Orchestrator-agnostic core**: The shared library (`dags/lib/`) implements the standard four-step ETL pattern (ingest, batch, process, store) without importing any orchestrator. Airflow and Prefect adapters are thin wrappers.
- **PostgreSQL analytics database**: Production and development databases on the same instance, with environment-variable configuration to switch between them.
- **Example pipeline included**: A working WID.world pipeline with both a `dag.py` (Airflow 3) and a `flow.py` (Prefect 3) entry point, demonstrating how to wire either orchestrator to the shared processing logic.

## Getting Started

### Prerequisites

- **PostgreSQL 14+** installed and running. Version 14 is the minimum because the IAM script uses `pg_read_all_data` and `pg_write_all_data` predefined roles.
- **Python 3.10+** with pip.
- **One orchestrator** (optional for local testing), such as:
  - [Prefect 3](https://docs.prefect.io/v3/get-started/install): `pip install 'prefect>=3.0'`
  - [Airflow 3 via Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli): requires [Docker Desktop](https://www.docker.com/products/docker-desktop/) (macOS/Windows) or [Docker Engine](https://docs.docker.com/engine/install/) (Linux). See [Running Pipelines](#airflow-3-via-astro-cli) for configuration tips (port conflicts, database connection).

Uncomment your chosen orchestrator in [`requirements.txt`](requirements.txt) before running `make venv` (see [Python Environment](#python-environment)).

### Clone and Detach

Clone the scaffold, detach from this upstream, and push to your own remote to get an independent repository of data pipelines that you own and extend.

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
```

The pipeline code reads these variables at runtime to connect to the database. Edit `.env` to fill in `SQL_DB_PASSWORD` and adjust any other values. The template is commented and ships with defaults matching the DDL files (`lens` database, `data_pipelines` service user). If you rename databases or users in the DDL, update `.env` to match. The file is gitignored.

`SQL_DB_HOST` depends on how you run pipelines. Use `localhost` when running directly on the host (Prefect, standalone scripts). When the orchestrator runs inside Docker (Airflow via Astro CLI), containers cannot reach `localhost` on the host machine, so use `host.docker.internal` (macOS/Windows) or `172.17.0.1` (Linux).

#### Initialize the database

The script `iam.sql` creates:

- **`readers` role**: read-only permission bundle (via `pg_read_all_data`).
- **`read_only` user**: login for BI tools, notebooks, ad-hoc SQL; granted `readers`.
- **Pipeline service user**: login for pipeline read+write; granted `pg_read_all_data` + `pg_write_all_data`.

Grants are blanket across all non-system schemas (including future ones), so no further IAM changes are needed when adding a new pipeline schema.

Replace the two `<REDACTED>` password placeholders with real passwords, then run:

```bash
# Step 1: Create the databases (production + development).
psql -U postgres -d postgres -f database.ddl

# Steps 2-4 must be run against EACH database you use (e.g., lens and lens_dev).
# Schemas, IAM, and tables are per-database objects. Forgetting one database
# causes runtime errors when pipelines target it.

# Step 2: Initialize schemas and extensions.
psql -U postgres -d lens -f schemas.ddl
psql -U postgres -d lens_dev -f schemas.ddl

# Step 3: Create roles, users, and permissions.
psql -U postgres -d lens -f iam.sql
psql -U postgres -d lens_dev -f iam.sql

# Step 4: Create pipeline tables.
psql -U postgres -d lens -f dags/pipelines/example/tables.ddl
psql -U postgres -d lens_dev -f dags/pipelines/example/tables.ddl
```

If your PostgreSQL `pg_hba.conf` requires password authentication, connect using `psql -h localhost` to trigger password prompts.

> **Note:** The [TimescaleDB](https://docs.timescale.com/self-hosted/latest/install/) extension statements in `schemas.ddl` are commented out by default. If you have TimescaleDB installed, uncomment the `CREATE EXTENSION` statements before running Step 2.

### Python Environment

Uncomment your orchestrator in [`requirements.txt`](requirements.txt) (Airflow, Prefect, or both), then create the virtual environment. As you build pipelines, add any new dependencies to `requirements.txt` and re-run `make venv`.

```bash
# Create .venv/ and install all dependencies (core + dev).
make venv

# Activate the venv. make targets work without this, but running
# pipelines directly (e.g. python -m pipelines.example.flow) requires it.
source .venv/bin/activate
```

Set up pre-commit hooks:

```bash
pre-commit install
```

Run the test suite to verify your setup:

```bash
make test
```

The test suite connects to a PostgreSQL instance on `localhost:5432` using the `postgres` database with throwaway tables. Override the connection via `TEST_DB_URL` in your `.env` file if needed.

## Running Pipelines

The scaffold ships with adapters for [Apache Airflow](https://airflow.apache.org/) and [Prefect](https://www.prefect.io/), but the core pipeline logic is orchestrator-agnostic and can work with other orchestrators too. The sections below cover configuration tips for each of the included adapters.

### Airflow 3 via Astro CLI

Install the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) and [Docker Desktop](https://www.docker.com/products/docker-desktop/) (or [Docker Engine](https://docs.docker.com/engine/install/) on Linux).

`astro dev init` inside this repo creates `.astro/`, `Dockerfile`, etc. Astro's default DAGs folder is `dags/`, which matches this layout with no overrides needed. Delete the placeholder `dags/exampledag.py` that `astro dev init` drops in, since real DAGs are under `dags/pipelines/<name>/dag.py`.

After running `astro dev init`, configure two files:

1. **`.astro/config.yaml`**: Astro's internal metadata database defaults to port 5432, which conflicts with the analytics database. Change it to a different port (e.g., 5434). The webserver port (default 8080) may also conflict with other services:

    ```yaml
    postgres:
        port: "5434"
    webserver:
        port: "8081"
    ```

2. **`docker-compose.override.yml`**: Pass database connection variables (`SQL_DB_*`) and mount the data directory into the scheduler container. Use `host.docker.internal` (Mac/Windows) or `172.17.0.1` (Linux) for `SQL_DB_HOST` to reach the host database from inside the container:

    ```yaml
    services:
      scheduler:
        environment:
          - SQL_DB_HOST=host.docker.internal
          - SQL_DB_PORT=${SQL_DB_PORT}
          - SQL_DB_NAME=${SQL_DB_NAME}
          - SQL_DB_USER=${SQL_DB_USER}
          - SQL_DB_PASSWORD=${SQL_DB_PASSWORD}
          - DATA_DIR=/usr/local/airflow/data
          - AIRFLOW__LOGGING__ENABLE_TASK_CONTEXT_LOGGER=False
        volumes:
          - ${DATA_DIR:-./data}:/usr/local/airflow/data

      dag-processor:
        environment:
          - DATA_DIR=/usr/local/airflow/data
    ```

The `${VAR}` references are resolved from the `.env` file in the project root. If a variable is unset, it resolves to an empty string. Make sure your `.env` has all `SQL_DB_*` values filled in before running `astro dev start`.

### Prefect

Prefect does not care about the directory name. Putting flows under `dags/` keeps both orchestrators working with zero configuration overrides.

Start a local Prefect server (runs on `http://127.0.0.1:4200`):

```bash
pip install 'prefect>=3.0'
prefect server start
```

The Prefect web UI is available at `http://127.0.0.1:4200` once the server is running.

Run a flow against the local server:

```bash
PREFECT_API_URL=http://127.0.0.1:4200/api \
    PYTHONPATH=dags python -m pipelines.example.flow
```

Deploy to a Prefect server or Cloud from the `dags/` directory:

```bash
cd dags
prefect deploy pipelines/example/flow.py:flow \
    --name example-prod --work-pool default
```

## Database Environments

`database.ddl` creates two databases: one for production and one for development. Both share the same schemas, tables, and IAM configuration. Run `schemas.ddl`, `iam.sql`, and your pipeline DDL scripts against each database you use.

To switch between them, change `SQL_DB_NAME` in your `.env` file (or set `DATABASE_URL`):

- **Pipeline development** (running DAGs/flows locally): point `SQL_DB_NAME` at the development database.
- **Production** (deployed orchestrator): point `SQL_DB_NAME` at the production database.
- **Unit tests** (pytest): tests use the default `postgres` database with throwaway tables, configured via `TEST_DB_URL` in `conftest.py`. No pipeline schemas needed.

## Standard Pipeline

### Pipeline Sequence

The standard pipeline pattern is a five-step sequence. Extract and process are pipeline-specific (you write them); ingest, batch, and store are provided by the shared library. Extract is optional: if files are produced externally and pushed to the `ingest/` directory, the pipeline can start at ingest. You can follow this pattern as-is or adapt it to your needs:

1. **Extract** *(optional)*: Fetch data from an external source (API, file download, etc.) and save raw files to the `ingest/` directory.
2. **Ingest**: Route files from `ingest/` to `process/` or `store/` based on regex patterns in the configuration. Raises `RuntimeError` when no files are found.
3. **Batch**: Group files by timestamp into `FileSet` objects, then distribute them across batches respecting `max_process_tasks` and `min_file_sets_in_batch` configuration.
4. **Process**: Run the pipeline's `Processor` subclass on each batch. Each `FileSet` gets an independent database transaction. Failures are captured per-FileSet without affecting others. Batches run in parallel (Airflow dynamic task mapping / Prefect `.map()`).
5. **Store**: Move successfully processed files to `store/` and failed files to `quarantine/`.

### Example: Airflow DAG

```python
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
from lib.prefect_utils import PrefectETLConfig, create_standard_flow

from .constants import MyFileTypes
from .extract import extract
from .process import MyProcessor

config = PrefectETLConfig(
    pipeline_id="my_pipeline",
    pipeline_print_name="My Pipeline",
    description="Description of what this pipeline does.",
    file_types=MyFileTypes,
    processor_class=MyProcessor,
    process_format=r".*\.json$",
    db_schema="my_schema",
)

flow = create_standard_flow(
    config,
    extract_callable=extract,
    extract_kwargs={"ingest_dir": config.data_dirs.ingest},
)

if __name__ == "__main__":
    flow()
```

## Data Directories

Each pipeline uses four directories under `data/{pipeline_id}/`:

| Directory    | Purpose                                              |
|--------------|------------------------------------------------------|
| `ingest/`    | Drop raw files here to trigger processing.           |
| `process/`   | Staging area while processing is active.             |
| `store/`     | Archive for successfully processed files.            |
| `quarantine/`| Isolation for files that caused errors.              |

`data/` is gitignored. Set `DATA_DIR` in `.env` to use a different base path. Relative paths (e.g., `./data`) work and resolve from the project root. Docker Compose does not expand `~`, so use `./relative` or `/absolute` paths.

## Repository Structure

The top-level folder is called `dags/` because Astro CLI and native Airflow both hardcode that name as their DAGs folder. Airflow/Astro automatically adds `dags/` to `PYTHONPATH`, so `from lib.xxx import yyy` resolves without setup.

### [dags/lib/](dags/lib)

Shared utilities that are orchestrator-agnostic, plus thin adapter modules for Airflow and Prefect. Pipelines import from `lib.*` and never reference orchestrator APIs directly (except in `dag.py` and `flow.py` entry points).

- **[`etl_config.py`](dags/lib/etl_config.py)**: `ETLConfig` base dataclass providing unified configuration for pipeline parameters, file processing settings, database connections, and directory management with validation and sensible defaults.

- **[`task_utils.py`](dags/lib/task_utils.py)**: Orchestrator-agnostic implementations of the four standard tasks (`ingest`, `batch`, `process_wrapper`, `store`) and the abstract `Processor` base class. This is where the ETL pattern lives. Each task function accepts an `ETLConfig` and operates on files and database sessions without importing Airflow or Prefect.

- **[`filesystem_utils.py`](dags/lib/filesystem_utils.py)**: `DataState` enum for pipeline data states (ingest, process, store, quarantine), `ETLDataDirectories` for standardized directory management, and `FileSet` for coordinating file processing across different file types with JSON serialization support.

- **[`sql_utils.py`](dags/lib/sql_utils.py)**: Database utilities including `get_engine()` for creating engines from environment variables, `make_base()` for schema-scoped declarative bases with optional timestamp columns, `fkey()` for fully-qualified foreign key references, and `upsert_model_instances()` for bulk `INSERT ... ON CONFLICT` operations.

- **[`airflow_utils.py`](dags/lib/airflow_utils.py)**: Thin Airflow adapter. `AirflowETLConfig` extends `ETLConfig` with DAG-specific parameters (schedule, timeouts, retries, callbacks). `create_dag()` factory assembles the standard four-task DAG with dynamic task mapping for parallel processing.

- **[`prefect_utils.py`](dags/lib/prefect_utils.py)**: Thin Prefect adapter. `PrefectETLConfig` extends `ETLConfig` with flow-specific parameters (retries, timeouts). `create_standard_flow()` factory assembles the standard pipeline flow with `.map()` for parallel processing and optional extract callable.

- **[`logging_utils.py`](dags/lib/logging_utils.py)**: Logging utilities that work in both orchestrator and standalone contexts.

### [dags/pipelines/](dags/pipelines)

Contains a subdirectory for each pipeline. The included `example/` pipeline (World Inequality Database) demonstrates how to structure pipeline code, use the shared utility modules, and add an `extract` task that fetches data from data sources. It includes:

- `constants.py`: `FileType` enum of compiled regex patterns and pipeline-specific configuration.
- `extract.py`: Data extraction logic (API calls, file downloads, etc.).
- `process.py`: `Processor` subclass with domain logic for parsing files, building ORM instances, and upserting to the database.
- `sqla_models.py`: SQLAlchemy ORM models matching the DDL.
- `tables.ddl`: SQL definitions for this pipeline's tables, indexes, and comments.
- `dag.py`: Airflow 3 entry point.
- `flow.py`: Prefect 3 entry point.

### [database.ddl](database.ddl)

Creates the production and development databases. Must be executed first, before any schema or table initialization. Edit the database names to match your project.

### [schemas.ddl](schemas.ddl)

Creates PostgreSQL extensions and per-pipeline schemas. Add one `CREATE SCHEMA` block per pipeline. The TimescaleDB extension is commented out by default; uncomment it if installed.

### [iam.sql](iam.sql)

Identity and access management configuration. Creates a read-only role/user for data consumers and a pipeline service user with read+write access. Uses PostgreSQL 14+ predefined roles (`pg_read_all_data`, `pg_write_all_data`) for blanket permissions across all schemas. Replace the `<REDACTED>` password placeholders before running.

### [tests/](tests)

[Pytest](https://docs.pytest.org/en/stable/)-based test suite. Tests use a local PostgreSQL instance with throwaway tables in the `postgres` database, independent of pipeline schemas. Run with:

```bash
make test
```

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
