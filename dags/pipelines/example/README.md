# Data Pipeline: World Inequality Database (WID)

## Context

This document describes the ETL data pipeline which processes data sourced from the [World Inequality Database (WID.world)](https://wid.world/). The pipeline collects income and wealth distribution data covering 323 countries and entities, with some series going back to the 1800s. Data is fetched from the WID.world REST API, which provides free access using a shared API key bundled with the official [WID R package](https://github.com/WIDworld/wid-r-tool) (no user-specific authentication required).

The goal of this pipeline is to support downstream data consumers in generating analytics that provide insights into global inequality trends, wealth concentration patterns, and income distribution dynamics across countries and time periods.

The data includes:

| Dataset | Content | API Endpoint |
|---------|---------|--------------|
| **Countries** | Entity codes and names for all 323 WID entities (sovereign nations, sub-national regions, aggregate groupings). | `countries-available-variables` and `countries-variables-metadata` |
| **Variable Metadata** | Descriptions, units, population types, and age groups for each tracked distribution metric (sixlet code). | `countries-variables-metadata` |
| **Observations** | Distribution values (shares) per country, variable, percentile, and year, plus per-country data quality metadata (quality scores, imputation methods, extrapolation ranges). | `countries-variables` |

API documentation: [wid.world/codes-dictionary](https://wid.world/codes-dictionary/)

### Variables

This pipeline tracks two distribution metrics:

| Sixlet   | Concept                    | Series Type |
|----------|----------------------------|-------------|
| `sptinc` | Pre-tax national income    | Share       |
| `shweal` | Net personal wealth        | Share       |

### Percentiles

40 percentile ranges with progressively finer granularity toward the top:

- **Deciles** (10 ranges): p0p10 through p90p100.
- **Percentiles** (10 ranges): p90p91 through p99p100.
- **Milles** (10 ranges): p99p99.1 through p99.9p100.
- **Basis points** (10 ranges): p99.9p99.91 through p99.99p100.

## Pipeline

This pipeline includes both an [Airflow 3 DAG](dag.py) and a [Prefect 3 flow](flow.py) entry point to demonstrate how the same pipeline logic runs under either orchestrator. In practice, you would keep only the orchestrator you use and delete the other. Both implementations share the extraction, processing, and storage logic described in this section. The orchestrator-specific configuration and task wiring are covered in the [Airflow DAG](#airflow-dag) and [Prefect Flow](#prefect-flow) sections.

- Pipeline ID: `example`
- Schedule: `None` (manual trigger only).
- Task dependency: `extract >> ingest >> batch >> process >> store`

### Extract task

[Code](extract.py)

The extract task calls the [`extract()`](extract.py) function, which downloads data from the WID.world REST API and saves JSON files to the ingest directory.

**Incremental extraction:** The function queries `SELECT MAX(year) FROM observation` against the pipeline's [database schema](tables.ddl) to determine what data already exists. On first run (empty database or missing table), all available years are downloaded. On subsequent runs, only data from the max stored year onward is re-extracted (inclusive, since WID may revise recent estimates).

**Extraction sequence:**

1. **Country codes**: Fetches all entity codes available in WID for pre-tax income (`sptinc`) via the `countries-available-variables` endpoint. Returns ~323 sorted two-letter codes.

2. **Variable metadata**: Queries `countries-variables-metadata` for each sixlet (`sptinc`, `shweal`) using a single country (`US`) since metadata is the same regardless of country. Falls back to hardcoded descriptions from [`VARIABLES`](constants.py) if the API call fails.

3. **Country names**: Fetches entity names from the metadata API in batches of 50, parsing names from the `units` section of the response.

4. **Observations**: Fetches distribution data in batches of 20 countries from the `countries-variables` endpoint. Each batch queries all 80 API variable codes (2 sixlets x 40 percentiles, each suffixed with age code `992` and population code `j`). The response is flattened into individual observation records and deduplicated data quality records. A 1 second delay is applied between batches.

**Output files:**

- `countries_{timestamp}.json`: Array of `{country_code, name}` objects.
- `variable_metadata_{timestamp}.json`: Array of variable metadata dicts with `variable_code`, `concept`, `series_type`, `short_name`, `description`, `technical_description`, `unit`, `population_type`, `age_group`.
- `observations_{batch_idx}_{timestamp}.json`: Object with `observations` (array of `{country_code, variable_code, percentile_code, year, value}`) and `data_quality` (array of `{country_code, variable_code, quality_score, imputation, extrapolation_ranges}`).

**API resilience:**

- 3 retries with exponential backoff (3s, 6s, 9s) for HTTP and timeout errors.
- Automatic handling of WID `payload_too_large` responses, which return a `download_url` pointing to an S3-hosted file instead of inline JSON.
- Per-batch error isolation: a failed batch logs the exception and continues with remaining batches.

### Ingest task

The ingest task uses the standard [`ingest()`](../../lib/task_utils.py) function from the [Standard Pipeline](../../../README.md#standard-pipeline) pattern. It routes files from the `ingest/` directory to `process/` or `store/` based on the `process_format` regex (`.*\.json$`). All JSON files are moved to `process/` for downstream batching.

### Batch task

The batch task uses the standard [`batch()`](../../lib/task_utils.py) function. Files are grouped by timestamp into `FileSet` objects using the [`WIDFileTypes`](constants.py) enum (COUNTRY, VARIABLE_META, OBSERVATION), then distributed across processing batches. Each `FileSet` coordinates related files (one countries file, one variable metadata file, and one or more observation files sharing the same extraction timestamp).

### Process task

[Code](process.py)

The process task uses the [`WIDProcessor`](process.py) class, which inherits from the base [`Processor`](../../lib/task_utils.py) class. It provides specialized processing logic for seeding dimension tables and upserting observations.

**Database schema integration:**

- Database tables defined in [`tables.ddl`](tables.ddl).
- SQLAlchemy ORM models in [`sqla_models.py`](sqla_models.py) extending the base class from [`sql_utils.make_base()`](../../lib/sql_utils.py) with `schema="wid"` and `include_update_ts=True`.

The schema contains 5 tables organized into dimensions, facts, and metadata:

**Dimensions (3 tables)**

```
country (entity dimension)
variable (metric dimension)
percentile (distribution bracket dimension)
```

**Facts (1 table)**

```
observation (distribution values)
├── FK → country.country_code
├── FK → variable.variable_code
└── FK → percentile.percentile_code
```

*Unique index: `(country_code, variable_code, percentile_code, year)`*

**Metadata (1 table)**

```
data_quality (estimation methodology)
├── FK → country.country_code
└── FK → variable.variable_code
```

*Unique index: `(country_code, variable_code)`*

**Processing flow:**

The `process_file_set` method orchestrates processing in a specific sequence to ensure referential integrity: dimension tables are populated before the fact table that references them.

#### 1. Percentile dimension seeding ([`_seed_percentiles`](process.py))

Executed first on every run to ensure the percentile dimension exists before observations reference it. Creates 40 `Percentile` ORM instances from the [`PERCENTILES`](constants.py) constant.

- **Target table**: `wid.percentile`.
- **Database method**: [`upsert_model_instances`](../../lib/sql_utils.py) with `on_conflict_update=False` (insert-only, `ON CONFLICT DO NOTHING`). Safe to call on every run.
- **Data source**: Hardcoded in [`constants.py`](constants.py). Each tuple defines `(percentile_code, lower_bound, upper_bound, width, granularity)`.

#### 2. Variable metadata processing ([`_process_variable_meta`](process.py))

Processes variable metadata files to populate the variable dimension. Must complete before observations, since `observation.variable_code` references `variable.variable_code`.

- **Target table**: `wid.variable`.
- **Database method**: [`upsert_model_instances`](../../lib/sql_utils.py) with `on_conflict_update=True` and `conflict_columns=["variable_code"]`. Updates `short_name`, `description`, `technical_description`, `unit`, `population_type`, `age_group` on conflict. Immutable fields (`variable_code`, `concept`, `series_type`) are not included in `update_columns`.
- **Data processing**: Reads the variable metadata JSON array. Each record maps directly to a `Variable` ORM instance with 9 fields. Optional fields (`description`, `technical_description`, `population_type`, `age_group`) use `.get()` and default to None.

#### 3. Country processing ([`_process_countries`](process.py))

Processes country files to populate the country dimension. Must complete before observations, since `observation.country_code` references `country.country_code`.

- **Target table**: `wid.country`.
- **Database method**: [`upsert_model_instances`](../../lib/sql_utils.py) with `on_conflict_update=True` and `conflict_columns=["country_code"]`. Updates only `name` on conflict.
- **Data processing**: Reads the countries JSON array. Each record maps to a `Country` ORM instance with `country_code` (primary key) and `name`.

#### 4. Observation and data quality processing ([`_process_observations`](process.py))

Processes observation files, which contain both the fact records and quality metadata.

- **Target tables**: `wid.observation` and `wid.data_quality`.
- **JSON file structure**: Object with two keys: `observations` (array of fact records) and `data_quality` (array of quality metadata records).
- **Observation upsert**: [`upsert_model_instances`](../../lib/sql_utils.py) with `on_conflict_update=True` and `conflict_columns=["country_code", "variable_code", "percentile_code", "year"]`. Updates only `value` on conflict, preserving `create_ts` on the original insert. Each record maps to an `Observation` ORM instance with 5 fields.
- **Data quality upsert**: [`upsert_model_instances`](../../lib/sql_utils.py) with `on_conflict_update=True` and `conflict_columns=["country_code", "variable_code"]`. Updates `quality_score`, `imputation`, `extrapolation_ranges` on conflict. Each record maps to a `DataQuality` ORM instance with 5 fields.

**Database upsert summary:**

The pipeline uses a single upsert method ([`upsert_model_instances`](../../lib/sql_utils.py)) with two conflict strategies:

| Table | Conflict columns | On conflict | Update columns |
|-------|-----------------|-------------|----------------|
| `percentile` | `percentile_code` | DO NOTHING | (none) |
| `variable` | `variable_code` | DO UPDATE | `short_name`, `description`, `technical_description`, `unit`, `population_type`, `age_group` |
| `country` | `country_code` | DO UPDATE | `name` |
| `observation` | `country_code`, `variable_code`, `percentile_code`, `year` | DO UPDATE | `value` |
| `data_quality` | `country_code`, `variable_code` | DO UPDATE | `quality_score`, `imputation`, `extrapolation_ranges` |

### Store task

The store task uses the standard [`store()`](../../lib/task_utils.py) function from the [Standard Pipeline](../../../README.md#standard-pipeline) pattern. Successfully processed files are moved to `store/` and failed files to `quarantine/`.

## Airflow DAG

[Code](dag.py)

The DAG uses [`AirflowETLConfig`](../../lib/airflow_utils.py) with [`WIDFileTypes`](constants.py) for file type coordination and a task execution timeout of 2 hours. The standard four-task DAG is assembled via [`create_dag(config)`](../../lib/airflow_utils.py), then an extract task is prepended using a `PythonOperator`:

```python
config = AirflowETLConfig(
    pipeline_id="example",
    pipeline_print_name="Example Pipeline",
    description="...",
    file_types=WIDFileTypes,
    processor_class=WIDProcessor,
    dag_schedule_interval=None,
    process_format=r".*\.json$",
    db_schema="wid",
    task_execution_timeout=timedelta(hours=2),
)

dag = create_dag(config)

with dag:
    task_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
        do_xcom_push=False,
        execution_timeout=timedelta(hours=2),
        op_kwargs={
            "ingest_dir": config.data_dirs.ingest,
            "db_schema": "wid",
        },
    )
    task_extract >> dag.get_task("ingest")
```

Trigger the `example` DAG manually from the Airflow UI.

## Prefect Flow

[Code](flow.py)

The flow uses [`PrefectETLConfig`](../../lib/prefect_utils.py) with the same pipeline parameters as the Airflow DAG. It is built inside a `_build_flow()` factory function to defer Prefect imports until runtime. Each ETL step is wrapped as a Prefect `@task` with `cache_policy=NONE` (no result caching between runs).

```python
config = PrefectETLConfig(
    pipeline_id="example",
    pipeline_print_name="Example Pipeline",
    description="...",
    file_types=WIDFileTypes,
    processor_class=WIDProcessor,
    process_format=r".*\.json$",
    db_schema="wid",
    tags=["example", "wid", "inequality"],
)
```

**Differences from the Airflow DAG:**

- **Parallel processing**: The process task uses `.map(batches)` to distribute batches across concurrent Prefect task runs. In Airflow, this is handled by dynamic task mapping via `create_dag()`.
- **Early exit on empty ingest**: The `_ingest` task catches `RuntimeError` (no files found) and returns 0, which causes the flow to exit early without error. In Airflow, the ingest task raises and the DAG run fails.
- **Error collection**: The `_store` task collects process results using `f.result(raise_on_failure=False)` to ensure store runs even if individual batches failed.

**Running locally:**

```bash
PYTHONPATH=dags python -m pipelines.example.flow
```

**Deploying to Prefect server or Cloud:**

```bash
cd dags
prefect deploy pipelines/example/flow.py:flow --name example-prod --work-pool default
```
