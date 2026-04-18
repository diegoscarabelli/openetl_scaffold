"""
Airflow 3 DAG entry point for the example pipeline (WID).

Adds an extract task before the standard ingest/batch/process/store sequence. The
extract task fetches data from the WID.world API and saves JSON files to the ingest
directory. The pipeline is not scheduled and must be triggered manually.
"""

from datetime import timedelta

from airflow.providers.standard.operators.python import (
    PythonOperator,
)

from lib.airflow_utils import AirflowETLConfig, create_dag

from pipelines.wid.constants import WIDFileTypes
from pipelines.wid.extract import extract
from pipelines.wid.process import WIDProcessor

config = AirflowETLConfig(
    pipeline_id="example",
    pipeline_print_name="Example Pipeline",
    description=(
        "Extract, process, and store income and wealth "
        "distribution data from the World Inequality "
        "Database (WID.world)."
    ),
    file_types=WIDFileTypes,
    processor_class=WIDProcessor,
    dag_schedule_interval=None,
    process_format=r".*\.json$",
    db_schema="wid",
    task_execution_timeout=timedelta(hours=2),
)

# Create the standard four-task DAG.
dag = create_dag(config)

# Add an extract task before ingest.
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
        doc_md=(
            "Download data from WID.world for all "
            "countries and distribution percentiles. "
            "Files are saved as JSON to the ingest "
            "directory for downstream processing."
        ),
    )
    task_extract >> dag.get_task("ingest")
