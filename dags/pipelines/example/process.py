"""
Example processor — implement your domain logic here.

Rename ExampleProcessor to match your pipeline (e.g. LinkedInProcessor).
"""

from __future__ import annotations

import csv
from pathlib import Path

from sqlalchemy.orm import Session

from lib.filesystem_utils import FileSet
from lib.sql_utils import upsert_model_instances
from lib.task_utils import Processor

from .constants import ExampleFileTypes
from .sqla_models import ExampleRecord


class ExampleProcessor(Processor):
    """
    Replace with a name matching your pipeline.
    """

    def process_file_set(self, file_set: FileSet, session: Session) -> None:
        """
        Parse files and upsert rows into the database.

        Raise any exception to mark the batch as failed — the Processor base class
        catches it and the store task routes files to quarantine/.
        """
        for file_path in file_set.get_files(ExampleFileTypes.DATA):
            self._process_file(file_path, session)

    def _process_file(self, file_path: Path, session: Session) -> None:
        records = []
        with open(file_path, newline="") as f:
            for row in csv.DictReader(f):
                records.append(
                    ExampleRecord(
                        name=row["name"],
                        value=row.get("value", ""),
                    )
                )

        upsert_model_instances(
            session=session,
            model_instances=records,
            conflict_columns=["name"],
            on_conflict_update=True,
            update_columns=["value"],
        )
