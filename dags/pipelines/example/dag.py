"""
Airflow 3 DAG entry point.

Keep this file thin (~10 lines). All logic lives in process.py and lib/.

This file lives under `dags/pipelines/<name>/dag.py`, which Astro CLI and native Airflow
discover by default (both scan `dags/` recursively and add it to PYTHONPATH, so `from
lib.xxx import yyy` resolves without any extra setup).
"""

from datetime import datetime

from lib.airflow_utils import create_dag
from lib.pipeline_config import PipelineConfig

from .constants import ExampleFileTypes
from .process import ExampleProcessor

config = PipelineConfig(
    pipeline_id="example",
    print_name="Example Pipeline",
    description="Template pipeline — copy pipelines/example/ to start a new pipeline.",
    file_types=ExampleFileTypes,
    processor_class=ExampleProcessor,
    schedule=None,  # None = manual trigger only.
    start_date=datetime(2024, 1, 1),
    process_format=r".*\.csv$",
)

dag = create_dag(config)
