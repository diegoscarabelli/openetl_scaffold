"""
Prefect flow factory and PrefectETLConfig.

This module provides Prefect-specific configuration and flow assembly for the
ETL pipeline workflow. It includes:
    - PrefectETLConfig dataclass extending ETLConfig with flow/task parameters.
    - create_flow() factory for assembling complete flows from configuration.
    - Integration with task_utils for orchestrator-agnostic task logic.

Import PrefectETLConfig and create_flow() in your pipeline's flow.py.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable, List, Optional

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
    (retries as int seconds, cache policies, tags).
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

    # Tags for organizing and filtering flows in the Prefect UI.
    tags: List[str] = field(default_factory=list)

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


def create_flow(config: PrefectETLConfig) -> Callable:
    """
    Build the standard four-task Prefect flow from a PrefectETLConfig.

    Uses the task callables defined in task_utils. The flow returns early when no files
    are found (ingest returns 0), logging a message instead of failing.

    :param config: Prefect-specific pipeline configuration.
    :return: A Prefect flow callable.
    """

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
        count = _ingest()
        if not count:
            logger.info("No files to process. Exiting flow.")
            return
        batches = _batch()
        process_futures = _process.map(batches)
        all_results = [f.result(raise_on_failure=False) for f in process_futures]
        _store(all_results)

    return pipeline_flow
