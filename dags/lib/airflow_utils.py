"""Airflow 3 DAG factory. Import create_dag() in your pipeline's dag.py."""
from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from lib.task_utils import batch, ingest, process_wrapper, store

if TYPE_CHECKING:
    from lib.pipeline_config import PipelineConfig


def create_dag(config: "PipelineConfig") -> DAG:
    """Wire the standard ingest -> batch -> process -> store task sequence.

    The ingest task raises RuntimeError when no files are found; this is
    translated to AirflowSkipException so the run is marked skipped, not failed.
    """

    def _ingest(**context: Any) -> int:
        try:
            fn = config.ingest_callable or ingest
            return fn(config)
        except RuntimeError:
            from airflow.exceptions import AirflowSkipException
            raise AirflowSkipException("No files to process.")

    def _batch(**context: Any) -> list:
        fn = config.batch_callable or batch
        return fn(config)

    def _process_wrapper(serialized_batch: str, **context: Any) -> dict:
        run_id = context.get("run_id", "")
        return process_wrapper(serialized_batch, config, run_id=run_id)

    def _store(all_results: list, **context: Any) -> dict:
        fn = config.store_callable or store
        return fn(all_results, config)

    with DAG(
        dag_id=config.pipeline_id,
        description=config.description,
        schedule=config.schedule,
        start_date=config.start_date,
        catchup=False,
        max_active_runs=1,
        default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
        tags=[config.pipeline_id],
    ) as dag:
        task_ingest = PythonOperator(
            task_id="ingest",
            python_callable=_ingest,
        )
        task_batch = PythonOperator(
            task_id="batch",
            python_callable=_batch,
            trigger_rule="none_failed",
        )
        # Dynamic task mapping: one process task instance per batch.
        # Airflow 3 collects all mapped outputs into a list for the store task.
        task_process = PythonOperator.partial(
            task_id="process",
            python_callable=_process_wrapper,
        ).expand(op_args=task_batch.output)
        task_store = PythonOperator(
            task_id="store",
            python_callable=_store,
            op_args=[task_process.output],
        )
        task_ingest >> task_batch >> task_process >> task_store

    return dag
