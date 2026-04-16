# Example Pipeline

This is a template. Copy this directory to `pipelines/{your_pipeline}/` and:

1. Rename `ExampleFileTypes` in `constants.py` and define your regex patterns.
2. Rename `ExampleProcessor` in `process.py` and implement `process_file_set()`.
3. Rename `ExampleRecord` in `sqla_models.py` and define your ORM columns.
4. Update `tables.ddl` to match your ORM models.
5. Update `dag.py` or `flow.py` with your pipeline ID and config.
6. Add `CREATE SCHEMA IF NOT EXISTS {your_pipeline};` to `schemas.ddl`.
7. Run `psql -d system2_pipelines_db -f pipelines/{your_pipeline}/tables.ddl`.
8. Grant permissions to `system2_pipelines` (see top-level README).

## Triggering a run

Drop files matching `process_format` into `data/example/ingest/` and trigger
the DAG (Airflow) or run the flow (Prefect).
