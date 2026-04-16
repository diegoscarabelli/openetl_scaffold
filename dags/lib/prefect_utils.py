"""
Prefect flow factory.

Import create_flow() in your pipeline's flow.py.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lib.pipeline_config import PipelineConfig


def create_flow(config: "PipelineConfig"):
    """
    Build the standard four-task Prefect flow from a PipelineConfig.

    The ingest task returns 0 when no files are found; the flow then raises Abort to
    cancel cleanly without marking the run as failed.
    """
    from prefect import flow, task
    from prefect.cache_policies import NONE

    from lib.task_utils import batch, ingest, process_wrapper, store

    @task(cache_policy=NONE, name=f"{config.pipeline_id}.ingest")
    def _ingest() -> int:
        fn = config.ingest_callable or ingest
        try:
            return fn(config)
        except RuntimeError:
            return 0

    @task(cache_policy=NONE, name=f"{config.pipeline_id}.batch")
    def _batch() -> list:
        fn = config.batch_callable or batch
        return fn(config)

    @task(cache_policy=NONE, name=f"{config.pipeline_id}.process")
    def _process(serialized_batch: str) -> dict:
        return process_wrapper(serialized_batch, config)

    @task(cache_policy=NONE, name=f"{config.pipeline_id}.store")
    def _store(all_results: list) -> dict:
        fn = config.store_callable or store
        return fn(all_results, config)

    @flow(name=config.pipeline_id, description=config.description)
    def pipeline_flow() -> None:
        count = _ingest()
        if not count:
            from prefect.exceptions import Abort

            raise Abort("No files to process.")
        batches = _batch()
        process_futures = _process.map(batches)
        all_results = [f.result(raise_on_failure=False) for f in process_futures]
        _store(all_results)

    return pipeline_flow
