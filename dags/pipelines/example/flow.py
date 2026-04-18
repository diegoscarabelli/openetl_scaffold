"""
Prefect flow entry point for the example pipeline (WID).

Adds an extract task before the standard ingest/batch/process/store
sequence. The extract task fetches data from the WID.world API and
saves JSON files to the ingest directory.

Run locally (from the repo root; dags/ must be on PYTHONPATH):

    PYTHONPATH=dags python -m pipelines.example.flow

Deploy to a Prefect server:

    cd dags && prefect deploy pipelines/example/flow.py:flow \\
        --name example-prod --work-pool default
"""

from __future__ import annotations

from lib.prefect_utils import PrefectETLConfig, create_standard_flow

from pipelines.example.constants import WIDFileTypes
from pipelines.example.extract import extract
from pipelines.example.process import WIDProcessor

config = PrefectETLConfig(
    pipeline_id="example",
    pipeline_print_name="Example Pipeline",
    description=(
        "Extract, process, and store income and "
        "wealth distribution data from the World "
        "Inequality Database (WID.world)."
    ),
    file_types=WIDFileTypes,
    processor_class=WIDProcessor,
    process_format=r".*\.json$",
    db_schema="wid",
    task_timeout_seconds=7200,
)

flow = create_standard_flow(
    config,
    extract_callable=extract,
    extract_kwargs={
        "ingest_dir": config.data_dirs.ingest,
        "db_schema": "wid",
    },
)

if __name__ == "__main__":
    flow()
