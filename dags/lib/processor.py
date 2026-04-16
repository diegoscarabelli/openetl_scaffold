"""Processor ABC — subclass this in pipelines/{name}/process.py."""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from traceback import format_exc
from typing import TYPE_CHECKING, Dict, Optional

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from lib.filesystem_utils import FileSet
    from lib.pipeline_config import PipelineConfig


class Processor(ABC):
    """Abstract base class for pipeline processors.

    Implement process_file_set() with your domain logic. The process()
    template method handles DB session management and error capture.
    """

    def __init__(
        self,
        config: "PipelineConfig",
        run_id: str,
        start_date: datetime,
        file_set: "FileSet",
    ) -> None:
        self.config = config
        self.run_id = run_id
        self.start_date = start_date
        self.file_set = file_set

    def process(self) -> Dict[str, Optional[str]]:
        """Template method: open a DB session and call process_file_set().

        Returns {"success": True, "error": None} on success, or
                {"success": False, "error": <traceback>} on failure.
        The return value is used by the store task to route files.
        """
        from sqlalchemy.orm import Session
        from lib.sql_utils import get_engine

        engine = get_engine(schema=self.config.db_schema)
        try:
            with Session(engine) as session:
                self.process_file_set(self.file_set, session)
            return {"success": True, "error": None}
        except Exception:
            return {"success": False, "error": format_exc()}

    @abstractmethod
    def process_file_set(self, file_set: "FileSet", session: "Session") -> None:
        """Domain logic: parse files, build ORM instances, upsert to database.

        Raise any exception to signal failure. The template method captures it
        and routes files to quarantine/.
        """
        ...
