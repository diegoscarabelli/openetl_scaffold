"""
Prefect flow entry point for the WID pipeline.

Adds an extract task before the standard ingest/batch/process/store
sequence. The extract task fetches data from the WID.world API and
saves JSON files to the ingest directory.

Run locally (from the repo root; dags/ must be on PYTHONPATH):

    PYTHONPATH=dags python -m pipelines.wid.flow

Deploy to Prefect server or Cloud:

    cd dags && prefect deploy pipelines/wid/flow.py:flow \\
        --name wid-prod --work-pool default
"""

from __future__ import annotations

import logging
from typing import Callable, List

from lib.task_utils import batch, ingest, process_wrapper, store

from pipelines.wid.constants import WIDFileTypes
from pipelines.wid.extract import extract as extract_wid
from pipelines.wid.process import WIDProcessor

logger = logging.getLogger(__name__)


def _build_flow() -> Callable:
    """
    Build the WID Prefect flow with extract prepended.

    :return: A Prefect flow callable.
    """

    from prefect import flow, task
    from prefect.cache_policies import NONE

    from lib.prefect_utils import PrefectETLConfig

    config = PrefectETLConfig(
        pipeline_id="wid",
        pipeline_print_name="WID Pipeline",
        description=(
            "Extract, process, and store income and "
            "wealth distribution data from the World "
            "Inequality Database (WID.world)."
        ),
        file_types=WIDFileTypes,
        processor_class=WIDProcessor,
        process_format=r".*\.json$",
        db_schema="wid",
        tags=["wid", "inequality"],
    )

    @task(
        cache_policy=NONE,
        name="wid.extract",
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
        name="wid.ingest",
    )
    def _ingest() -> int:
        try:
            return ingest(config)
        except RuntimeError:
            return 0

    @task(
        cache_policy=NONE,
        name="wid.batch",
    )
    def _batch() -> list:
        return batch(config)

    @task(
        cache_policy=NONE,
        name="wid.process",
    )
    def _process(
        serialized_file_sets: list,
    ) -> list:
        return process_wrapper(serialized_file_sets, config)

    @task(
        cache_policy=NONE,
        name="wid.store",
    )
    def _store(all_results: list) -> dict:
        return store(all_results, config)

    @flow(
        name="wid",
        description=config.description,
        tags=config.tags,
    )
    def wid_flow() -> None:
        """
        WID pipeline flow: extract, ingest, batch, process, store.
        """

        _extract()

        count = _ingest()
        if not count:
            logger.info("No files to process. Exiting flow.")
            return

        batches = _batch()
        futures = _process.map(batches)
        all_results: List = [f.result(raise_on_failure=False) for f in futures]
        _store(all_results)

    return wid_flow


flow = _build_flow()

if __name__ == "__main__":
    flow()
