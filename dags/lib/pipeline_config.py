"""
PipelineConfig — single source of truth for a pipeline's behavior.

__post_init__ derives db_schema and data_dirs from pipeline_id, so most pipelines only
need to set a handful of fields explicitly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Callable, Optional, Type

from lib.filesystem_utils import ETLDataDirectories


@dataclass
class PipelineConfig:
    pipeline_id: str
    """
    Matches the directory name under pipelines/; used as DB schema prefix.
    """

    print_name: str
    """
    Human-readable label for logs and UI.
    """

    description: str

    file_types: Type[Enum]
    """
    Enum subclass where each member is a compiled regex pattern.
    """

    processor_class: type
    """
    Processor subclass that implements process_file_set().
    """

    schedule: Optional[str]
    """
    Cron string (e.g. '0 6 * * *'), or None for manual-trigger-only.
    """

    start_date: datetime
    """
    Earliest logical date; used by Airflow, informational for Prefect.
    """

    process_format: str
    """Regex: files matching this are routed from ingest/ to process/."""

    store_format: str = ""
    """Regex: files matching this bypass processing and go directly to store/."""

    max_process_tasks: int = 4
    """
    Max concurrent process task instances.
    """

    min_file_sets_in_batch: int = 1
    """
    Min FileSets to accumulate before triggering a batch.
    """

    db_schema: str = ""
    """
    PostgreSQL schema — defaults to pipeline_id.

    Set in __post_init__.
    """

    data_dirs: ETLDataDirectories = field(default_factory=ETLDataDirectories)
    """
    Resolved data directory paths.

    Auto-populated in __post_init__.
    """

    ingest_callable: Optional[Callable] = None
    batch_callable: Optional[Callable] = None
    store_callable: Optional[Callable] = None

    def __post_init__(self) -> None:
        if not self.db_schema:
            self.db_schema = self.pipeline_id
        if not self.data_dirs.pipeline_id:
            self.data_dirs = ETLDataDirectories(pipeline_id=self.pipeline_id)
