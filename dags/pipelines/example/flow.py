"""Prefect flow entry point.

Keep this file thin (~10 lines). All logic lives in process.py and lib/.

Run locally (from the repo root; `dags/` must be on PYTHONPATH so
`from lib.xxx import yyy` resolves):

    PYTHONPATH=dags python -m pipelines.example.flow

Deploy to Prefect server or Cloud:

    cd dags && prefect deploy pipelines/example/flow.py:flow \\
        --name example-prod --work-pool default
"""
from datetime import datetime

from lib.pipeline_config import PipelineConfig
from lib.prefect_utils import create_flow

from .constants import ExampleFileTypes
from .process import ExampleProcessor

config = PipelineConfig(
    pipeline_id="example",
    print_name="Example Pipeline",
    description="Template pipeline — copy pipelines/example/ to start a new pipeline.",
    file_types=ExampleFileTypes,
    processor_class=ExampleProcessor,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    process_format=r".*\.csv$",
)

flow = create_flow(config)

if __name__ == "__main__":
    flow()
