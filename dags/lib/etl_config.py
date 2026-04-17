"""
Configuration management through the ETLConfig dataclass for data pipelines.

This module provides the base ETLConfig dataclass with orchestrator-agnostic
configuration for file processing, database connections, and directory management.
Orchestrator-specific subclasses live in airflow_utils.py and prefect_utils.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, Type

from lib.filesystem_utils import DefaultFileTypes, ETLDataDirectories


@dataclass
class ETLConfig:
    """
    Base configuration for pipeline tasks.

    Subclass as AirflowETLConfig or PrefectETLConfig to add orchestrator-specific
    parameters.
    """

    # Name of the pipeline. Used for naming data directories, DB
    # schema, and the orchestrator's top-level object (DAG or flow).
    pipeline_id: str

    # Callable functions used in tasks to override the default
    # functions defined in task_utils.py. The functions must have the
    # same signature as the default functions.
    ingest_callable: Optional[Callable] = None
    batch_callable: Optional[Callable] = None
    store_callable: Optional[Callable] = None

    # Callable class to be used in the process task. This must be a
    # subclass of the Processor class defined in lib.task_utils.
    processor_class: Optional[Type[Any]] = None

    # File types and the corresponding regex patterns to be used in
    # the FileSet class.
    # For example:
    # class FileTypes(Enum):
    #     DATA = re.compile(r".*data.*\.csv$")
    #     METADATA = re.compile(r".*metadata.*\.json$")
    #     CONFIG = re.compile(r".*config.*\.json$")
    file_types: Type[Enum] = DefaultFileTypes

    # Maximum number of process tasks to run in parallel.
    max_process_tasks: int = 1

    # Minimum number of file sets to process in a single process task.
    min_file_sets_in_batch: int = 1

    # Whether to enable autoflush for SQLAlchemy sessions.
    autoflush: bool = True

    # Extended name of the pipeline suitable for print statements.
    pipeline_print_name: Optional[str] = None

    # Description of the pipeline.
    description: Optional[str] = None

    # PostgreSQL schema. Defaults to pipeline_id.
    db_schema: str = ""

    # Directory paths for data associated with the pipeline.
    data_dirs: ETLDataDirectories = field(default_factory=ETLDataDirectories)

    # Link to analytics dashboard.
    dashboard_link: Optional[str] = None

    # Regular expressions for files to be moved to the process and
    # store directories during file ingestion, as implemented in the
    # ingest() function of task_utils.
    process_format: Optional[str] = None
    store_format: Optional[str] = None

    def __post_init__(self) -> None:
        """
        Post-initialization for ETLConfig.

        Sets default values for pipeline_print_name, db_schema, and data_dirs. Validates
        that max_process_tasks and min_file_sets_in_batch are >= 1. Raises ValueError if
        configuration is invalid.
        """

        if not self.pipeline_id:
            raise ValueError("pipeline_id must not be empty.")
        if self.max_process_tasks < 1:
            raise ValueError("max_process_tasks must be >= 1.")
        if self.min_file_sets_in_batch < 1:
            raise ValueError("min_file_sets_in_batch must be >= 1.")
        if not issubclass(self.file_types, Enum):
            raise ValueError("file_types must be an Enum subclass.")
        if self.pipeline_print_name is None:
            self.pipeline_print_name = self.pipeline_id
        if not self.db_schema:
            self.db_schema = self.pipeline_id
        self.data_dirs.set_paths(self.pipeline_id)

    def __str__(self) -> str:
        """
        Return a string representation of the ETLConfig.

        :return: Human-readable config summary.
        """

        return (
            f"ETLConfig(pipeline_id={self.pipeline_id}, "
            f"pipeline_print_name={self.pipeline_print_name})"
        )
