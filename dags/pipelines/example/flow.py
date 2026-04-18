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

import logging
from typing import List

from prefect import flow as prefect_flow
from prefect import task
from prefect.cache_policies import NONE

from lib.prefect_utils import PrefectETLConfig
from lib.task_utils import batch, ingest, process_wrapper, store

from pipelines.example.constants import WIDFileTypes
from pipelines.example.extract import extract as extract_wid
from pipelines.example.process import WIDProcessor

logger = logging.getLogger(__name__)

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
)


@task(
    cache_policy=NONE,
    name="example.extract",
    timeout_seconds=7200,
)
def _extract() -> int:
    paths = extract_wid(
        ingest_dir=config.data_dirs.ingest,
        db_schema="wid",
    )
    return len(paths)


@task(
    cache_policy=NONE,
    name="example.ingest",
)
def _ingest() -> int:
    try:
        return ingest(config)
    except RuntimeError:
        return 0


@task(
    cache_policy=NONE,
    name="example.batch",
)
def _batch() -> list:
    return batch(config)


@task(
    cache_policy=NONE,
    name="example.process",
)
def _process(
    serialized_file_sets: list,
) -> list:
    return process_wrapper(serialized_file_sets, config)


@task(
    cache_policy=NONE,
    name="example.store",
)
def _store(all_results: list) -> dict:
    return store(all_results, config)


@prefect_flow(
    name="example",
    description=config.description,
)
def flow() -> None:
    """Example pipeline flow: extract, ingest, batch, process, store."""
    _extract()

    count = _ingest()
    if not count:
        logger.info("No files to process. Exiting flow.")
        return

    batches = _batch()
    futures = _process.map(batches)
    all_results: List = [
        f.result(raise_on_failure=False) for f in futures
    ]
    _store(all_results)


if __name__ == "__main__":
    flow()
