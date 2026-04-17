"""
Airflow 3 DAG factory and AirflowETLConfig.

This module provides Airflow-specific configuration and DAG assembly for the ETL
pipeline workflow. It includes:
    - AirflowETLConfig dataclass extending ETLConfig with DAG parameters.
    - create_dag() factory for assembling complete DAGs from configuration.
    - Integration with task_utils for orchestrator-agnostic task logic.

Import AirflowETLConfig and create_dag() in your pipeline's dag.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Callable, Dict, Optional, Union

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.exceptions import AirflowSkipException

from lib.etl_config import ETLConfig
from lib.task_utils import batch, ingest, process_wrapper, store

# -----------------------------------------------------------------------
# AirflowETLConfig
# -----------------------------------------------------------------------


@dataclass
class AirflowETLConfig(ETLConfig):
    """
    Configuration parameters for Airflow DAGs and pipeline tasks.

    For a list of DAG-level parameters, see:
    https://www.astronomer.io/docs/learn/airflow-dag-parameters/
    """

    # Additional keyword arguments to be passed to the callable
    # functions used in tasks.
    extra_ingest_kwargs: Dict[str, Any] = field(default_factory=dict)
    extra_batch_kwargs: Dict[str, Any] = field(default_factory=dict)
    extra_processor_init_kwargs: Dict[str, Any] = field(default_factory=dict)
    extra_store_kwargs: Dict[str, Any] = field(default_factory=dict)

    # How long a DAG run should be up before timing out / failing,
    # so that new DAG runs can be created.
    dag_dagrun_timeout: timedelta = timedelta(hours=12)

    # Timestamp that the schedule_interval parameter is relative to.
    # Must be a fixed pendulum.datetime in the past.
    # See Airflow timezone docs for time-zone-aware DAGs.
    dag_start_date: pendulum.DateTime = pendulum.datetime(
        year=2025, month=1, day=1, tz="UTC"
    )

    # Time interval between consecutive DAG runs or cron expression,
    # which the DAG will use to automatically schedule runs. If None,
    # the DAG is not scheduled.
    dag_schedule_interval: Optional[Union[timedelta, str]] = None

    # The scheduler, by default, kicks off a DAG Run for any data
    # interval that has not been run since the last data interval
    # (or has been cleared). When catchup is False, the scheduler
    # creates a DAG run only for the latest interval.
    dag_catchup: bool = False

    # Maximum number of active DAG runs, beyond this number of DAG
    # runs in a running state, the scheduler won't create new active
    # DAG runs.
    dag_max_active_runs: int = 1

    # Task execution timeout. Tasks that exceed this duration will
    # be failed.
    task_execution_timeout: timedelta = timedelta(hours=1)

    # A function to be called when a task instance fails.
    dag_on_failure_callback: Optional[Callable] = None

    # Postgres username to be used for the pipeline.
    postgres_user: str = ""

    @property
    def dag_id(self) -> str:
        """
        Alias for pipeline_id, used in DAG construction.

        :return: The pipeline_id string.
        """

        return self.pipeline_id

    @property
    def dag_args(self) -> dict:
        """
        Return DAG arguments for DAG instantiation.

        Example::

            dag = DAG(**config.dag_args)

        :return: Dict of keyword arguments for the DAG constructor.
        """

        return {
            "dag_id": self.dag_id,
            "catchup": self.dag_catchup,
            "dagrun_timeout": self.dag_dagrun_timeout,
            "max_active_runs": self.dag_max_active_runs,
            "schedule": self.dag_schedule_interval,
            "start_date": self.dag_start_date,
            "description": self.description,
            "doc_md": self.description,
            "default_args": {
                "on_failure_callback": self.dag_on_failure_callback,
                "execution_timeout": self.task_execution_timeout,
            },
        }

    def __post_init__(self) -> None:
        """
        Post-initialization for AirflowETLConfig.

        Calls base validation, then sets Airflow-specific defaults.
        """

        super().__post_init__()
        if not self.postgres_user:
            self.postgres_user = f"airflow_{self.pipeline_id}"

    def __str__(self) -> str:
        """
        Return a string representation of the AirflowETLConfig.

        :return: Human-readable config summary.
        """

        return (
            f"AirflowETLConfig(dag_id={self.dag_id}, "
            f"pipeline_print_name="
            f"{self.pipeline_print_name})"
        )


# -----------------------------------------------------------------------
# DAG factory
# -----------------------------------------------------------------------


def create_dag(
    config: AirflowETLConfig,
    apply_default_task_sequence: bool = True,
) -> DAG:
    """
    Generate an Airflow DAG for the pipeline using the configuration.

    Uses the task callables defined in task_utils. The extra keyword
    arguments are passed to the task callables via the ``op_kwargs``
    parameter in order to allow dynamic configuration of the tasks at
    runtime (such as Jinja templated fields).

    If you want to set a different order for the tasks (or include
    additional tasks), you can do the following:

    1. Turn off ``apply_default_task_sequence`` to prevent the default
       task order from being set.
    2. Use ``dag.get_task(<task_id>)`` to get a task by its ID and set
       the order using ``task1 >> task2``.

    :param config: Airflow-specific pipeline configuration.
    :param apply_default_task_sequence: If True, sets the default task
        dependency sequence.
    :return: An Airflow DAG object with the standard task sequence.
    """

    def _ingest(config: AirflowETLConfig, **kwargs: Any) -> int:
        """
        Wrap ingest to convert RuntimeError to AirflowSkipException.
        """

        fn = config.ingest_callable or ingest
        try:
            return fn(config, **kwargs)
        except RuntimeError:
            raise AirflowSkipException("No files to process.")

    def _batch(config: AirflowETLConfig, **kwargs: Any) -> list:
        """
        Wrap batch output as tuples for op_args expansion.
        """

        fn = config.batch_callable or batch
        results = fn(config, **kwargs)
        # Wrap each batch in a tuple so that
        # expand(op_args=...) passes the entire list as one
        # positional arg per mapped instance.
        return [(b,) for b in results]

    dag = DAG(**config.dag_args)

    with dag:
        task_ingest = PythonOperator(
            task_id="ingest",
            python_callable=_ingest,
            op_kwargs={
                "config": config,
                **config.extra_ingest_kwargs,
            },
            doc_md=(
                f"Move files from the "
                f"{config.data_dirs.ingest} directory to the "
                f"{config.data_dirs.process} or "
                f"{config.data_dirs.store} directory, "
                f"depending on which arguments are specified "
                f"in the config parameter."
            ),
        )

        task_batch = PythonOperator(
            task_id="batch",
            python_callable=_batch,
            op_kwargs={
                "config": config,
                **config.extra_batch_kwargs,
            },
            doc_md=(
                "Construct batches of file sets from the "
                f"content of the {config.data_dirs.process} "
                "directory."
            ),
        )

        task_process = PythonOperator.partial(
            task_id="process",
            python_callable=process_wrapper,
            op_kwargs={
                "config": config,
                "run_id": "{{ dag_run.run_id }}",
                "start_date": "{{ dag_run.start_date }}",
                **config.extra_processor_init_kwargs,
            },
            doc_md=(
                "Dynamically generate one or more concurrent "
                "Airflow tasks, each of which processes a "
                "batch of file sets sequentially. This task "
                "extracts, validates, and inserts the data to "
                "the SQL database resources associated with "
                "this pipeline."
            ),
        ).expand(
            # Each element is a (serialized_batch,) tuple.
            op_args=task_batch.output
        )

        task_store = PythonOperator(
            task_id="store",
            python_callable=config.store_callable or store,
            op_kwargs={
                "config": config,
                **config.extra_store_kwargs,
            },
            op_args=[task_process.output],
            doc_md=(
                f"Move all files from the "
                f"{config.data_dirs.process} directory to the "
                f"{config.data_dirs.store} directory, except "
                f"those that failed processing, which are "
                f"moved to the "
                f"{config.data_dirs.quarantine} directory."
            ),
        )

        if apply_default_task_sequence:
            task_ingest >> task_batch >> task_process >> task_store

    return dag
