"""
Prefect flow factory and PrefectETLConfig.

This module provides Prefect-specific configuration and flow assembly for the
ETL pipeline workflow. It includes:
    - PrefectETLConfig dataclass extending ETLConfig with flow/task parameters.
    - create_standard_flow() factory for assembling complete standard flows.
    - Integration with task_utils for orchestrator-agnostic task logic.

Import PrefectETLConfig and create_standard_flow() in your pipeline's flow.py.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Optional

from prefect import flow, task
from prefect.cache_policies import NONE

from lib.etl_config import ETLConfig
from lib.task_utils import batch, ingest, process_wrapper, store

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------
# PrefectETLConfig
# -----------------------------------------------------------------------


@dataclass
class PrefectETLConfig(ETLConfig):
    """
    Configuration parameters for Prefect flows and tasks.

    Mirrors the structure of AirflowETLConfig but with Prefect-native parameters
    (retries as int seconds, cache policies, timeouts).
    """

    # Number of times to retry the flow on failure.
    flow_retries: int = 0

    # Seconds to wait between flow retries.
    flow_retry_delay_seconds: int = 0

    # Flow execution timeout in seconds. None means no timeout.
    flow_timeout_seconds: Optional[int] = None

    # Number of times to retry each task on failure.
    task_retries: int = 0

    # Seconds to wait between task retries.
    task_retry_delay_seconds: int = 0

    # Task execution timeout in seconds. None means no timeout.
    task_timeout_seconds: Optional[int] = None

    def __str__(self) -> str:
        """
        Return a string representation of the PrefectETLConfig.

        :return: Human-readable config summary.
        """

        return (
            f"PrefectETLConfig(pipeline_id={self.pipeline_id}, "
            f"pipeline_print_name={self.pipeline_print_name})"
        )


# -----------------------------------------------------------------------
# Flow factory
# -----------------------------------------------------------------------


def create_standard_flow(
    config: PrefectETLConfig,
    extract_callable: Optional[Callable] = None,
    extract_kwargs: Optional[dict] = None,
) -> Callable:
    """
    Build the standard Prefect flow from a PrefectETLConfig.

    Assembles the standard pipeline sequence (extract, ingest, batch,
    process, store) with each step wrapped as a Prefect task. The
    extract step is optional: when extract_callable is None, the flow
    starts at ingest. The flow returns early when no files are found
    (ingest returns 0), logging a message instead of failing.

    For flows that need a different task sequence or non-standard
    composition, assemble the flow manually using the task callables
    from task_utils and the Prefect @task/@flow decorators directly.

    :param config: Prefect-specific pipeline configuration.
    :param extract_callable: Optional callable to run before ingest.
        Wrapped as a Prefect task using the config's retry and timeout
        settings. Called with extract_kwargs if provided.
    :param extract_kwargs: Optional keyword arguments passed to the
        extract callable.
    :return: A Prefect flow callable.
    """

    if extract_callable is not None:

        @task(
            cache_policy=NONE,
            name=f"{config.pipeline_id}.extract",
            retries=config.task_retries,
            retry_delay_seconds=config.task_retry_delay_seconds,
            timeout_seconds=config.task_timeout_seconds,
        )
        def _extract() -> None:
            extract_callable(**(extract_kwargs or {}))

    @task(
        cache_policy=NONE,
        name=f"{config.pipeline_id}.ingest",
        retries=config.task_retries,
        retry_delay_seconds=config.task_retry_delay_seconds,
        timeout_seconds=config.task_timeout_seconds,
    )
    def _ingest() -> int:
        fn = config.ingest_callable or ingest
        try:
            return fn(config)
        except RuntimeError:
            return 0

    @task(
        cache_policy=NONE,
        name=f"{config.pipeline_id}.batch",
        retries=config.task_retries,
        retry_delay_seconds=config.task_retry_delay_seconds,
        timeout_seconds=config.task_timeout_seconds,
    )
    def _batch() -> list:
        fn = config.batch_callable or batch
        return fn(config)

    @task(
        cache_policy=NONE,
        name=f"{config.pipeline_id}.process",
        retries=config.task_retries,
        retry_delay_seconds=config.task_retry_delay_seconds,
        timeout_seconds=config.task_timeout_seconds,
    )
    def _process(serialized_file_sets: list) -> list:
        return process_wrapper(serialized_file_sets, config)

    @task(
        cache_policy=NONE,
        name=f"{config.pipeline_id}.store",
        retries=config.task_retries,
        retry_delay_seconds=config.task_retry_delay_seconds,
        timeout_seconds=config.task_timeout_seconds,
    )
    def _store(all_results: list) -> dict:
        fn = config.store_callable or store
        return fn(all_results, config)

    @flow(
        name=config.pipeline_id,
        description=config.description,
        retries=config.flow_retries,
        retry_delay_seconds=config.flow_retry_delay_seconds,
        timeout_seconds=config.flow_timeout_seconds,
    )
    def pipeline_flow() -> None:
        if extract_callable is not None:
            _extract()
        count = _ingest()
        if not count:
            logger.info("No files to process. Exiting flow.")
            return
        batches = _batch()
        process_futures = _process.map(batches)
        all_results = [f.result(raise_on_failure=False) for f in process_futures]
        _store(all_results)

    return pipeline_flow
