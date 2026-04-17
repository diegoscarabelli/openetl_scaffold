"""
Orchestrator-agnostic task implementations and Processor ABC.

This module provides standard task functions for the ETL pipeline workflow,
including file ingestion, batching, processing, and storage operations.
It includes:
    - ingest() for routing files from ingest/ to process/ or store/.
    - batch() for grouping files into FileSets and distributing batches.
    - process_wrapper() for deserializing batches and running the processor.
    - store() for routing processed files to store/ or quarantine/.
    - Processor ABC for custom data processing logic.

Wired into DAGs/flows via lib/airflow_utils.py or lib/prefect_utils.py.
"""

from __future__ import annotations

import random
import re
import shutil
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from traceback import format_exc
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from lib.etl_config import ETLConfig
from lib.filesystem_utils import DataState, FileSet
from lib.logging_utils import LOGGER


def ingest(config: ETLConfig, **kwargs: Any) -> int:
    """
    Scan ingest/ and route files to process/ or store/.

    Files matching store_format are moved to store/ first. Files
    matching process_format are moved to process/ (if not already
    moved). All remaining files are moved to process/ only if
    process_format is not specified. Otherwise, files not matching
    either format remain in ingest/.

    Raises RuntimeError when no files are found. Callers should
    translate this to the orchestrator's skip/cancel signal:
      - Airflow 3: raise AirflowSkipException.
      - Prefect: return early from the flow.

    :param config: Pipeline configuration.
    :param kwargs: Extra keyword arguments forwarded by the
        orchestrator (e.g. extra_ingest_kwargs). Ignored by the
        default implementation but available to custom
        ingest_callable replacements.
    :return: Number of files routed to process/.
    """

    dirs = config.data_dirs

    # Check existence of target directories.
    for state in [
        DataState.INGEST,
        DataState.PROCESS,
        DataState.STORE,
    ]:
        dir_path = getattr(dirs, state.value)
        if not dir_path.exists():
            raise FileNotFoundError(
                f"The '{state.value}' directory " f"does not exist."
            )

    files = set(dirs.ingest.glob("*"))
    if not files:
        raise RuntimeError(f"[{config.pipeline_id}] " f"No files found to ingest.")

    store_re = re.compile(config.store_format) if config.store_format else None
    process_re = re.compile(config.process_format) if config.process_format else None

    to_process = set()
    to_store = set()

    # First, move files matching store_format to store/.
    if store_re is not None:
        for f in list(files):
            if store_re.search(f.name):
                shutil.move(str(f), str(dirs.store / f.name))
                to_store.add(f.name)
                files.remove(f)

    # Then, move files matching process_format to process/.
    if process_re is not None:
        for f in list(files):
            if process_re.search(f.name):
                shutil.move(str(f), str(dirs.process / f.name))
                to_process.add(f.name)
                files.remove(f)

    # Move all remaining to process/ if process_format is unset.
    if config.process_format is None:
        for f in list(files):
            shutil.move(str(f), str(dirs.process / f.name))
            to_process.add(f.name)
            files.remove(f)

    # Log results.
    LOGGER.info(
        "Ingest results:\n"
        "Moved %d files to 'process': %s.\n"
        "Moved %d files to 'store': %s.\n"
        "Left %d files in 'ingest': %s.",
        len(to_process),
        sorted(to_process),
        len(to_store),
        sorted(to_store),
        len(files),
        sorted([f.name for f in files]),
    )

    return len(to_process)


def batch(config: ETLConfig, **kwargs: Any) -> List[List[str]]:
    """
    Group files in process/ into FileSets and distribute across batches.

    Files are grouped by timestamp extracted from their filenames. When no timestamp is
    found, the file's last-modified time is used with random jitter to avoid accidental
    grouping. Each group becomes a FileSet. FileSets are then distributed into batches
    respecting max_process_tasks and min_file_sets_in_batch.

    Override via config.batch_callable for custom grouping logic.

    :param config: Pipeline configuration.
    :param kwargs: Extra keyword arguments forwarded by the orchestrator (e.g.
        extra_batch_kwargs). Ignored by the default implementation but available to
        custom batch_callable replacements.
    :return: List of batches, where each batch is a list of JSON-serialized FileSets.
    """

    if config.batch_callable:
        return config.batch_callable(config, **kwargs)

    dirs = config.data_dirs
    file_types = list(config.file_types)

    # -----------------------------------------------------------
    # Construct FileSets grouped by timestamp.
    # -----------------------------------------------------------

    timestamp_regex = (
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}" r"(?:\.\d{1,6})?(?:[+-]\d{2}:\d{2}|Z)?"
    )
    files_by_dt: Dict[datetime, list] = {}
    file_paths = list(dirs.process.glob("*"))
    for f in file_paths:
        match = re.search(timestamp_regex, f.name)
        if match:
            ts_str = match.group(0).replace("Z", "+00:00")
            dt = datetime.fromisoformat(ts_str)
        else:
            # Use mtime with jitter to avoid accidental
            # grouping of unrelated files.
            dt = datetime.fromtimestamp(f.stat().st_mtime)
            dt = dt.replace(microsecond=random.randint(0, 999999))
        files_by_dt.setdefault(dt, []).append(f)
    files_by_dt = dict(sorted(files_by_dt.items()))

    file_sets: List[FileSet] = []
    for dt, paths in files_by_dt.items():
        file_set = FileSet()
        for f in paths:
            for ft in file_types:
                if ft.value.search(f.name):
                    if ft not in file_set.files:
                        file_set.files[ft] = []
                    file_set.files[ft].append(f)
                    break
        # Verify all files were matched to a file type.
        if set(file_set.file_paths) != set(paths):
            unmatched = [p.name for p in paths if p not in file_set.file_paths]
            raise ValueError(
                f"Not all files for dt={dt} were included "
                f"in the file set. "
                f"Unmatched files: {unmatched}."
            )
        if file_set.files:
            file_sets.append(file_set)

    if not file_sets:
        raise RuntimeError(f"[{config.pipeline_id}] " f"No file sets to process.")

    # -----------------------------------------------------------
    # Distribute FileSets into batches.
    # -----------------------------------------------------------

    num_file_sets = len(file_sets)
    batches: List[List[FileSet]] = []
    idx = 0

    # Create batches of min_file_sets_in_batch until we reach
    # max_process_tasks or run out of file sets.
    while (
        idx + config.min_file_sets_in_batch <= num_file_sets
        and len(batches) < config.max_process_tasks
    ):
        batch_slice = file_sets[idx : idx + config.min_file_sets_in_batch]
        batches.append(batch_slice)
        idx += config.min_file_sets_in_batch

    # Handle the case where no batches could be created.
    if not batches:
        batches = [list(file_sets)]
    else:
        # Round-robin remaining FileSets across batches.
        for i, fs in enumerate(file_sets[idx:]):
            batches[i % len(batches)].append(fs)

    # Serialize each batch for XCom / task.map compatibility.
    return [[fs.to_serializable() for fs in b] for b in batches]


def process_wrapper(
    serialized_file_sets: List[str],
    config: ETLConfig,
    run_id: str = "",
    start_date: Optional[datetime] = None,
    **kwargs: Any,
) -> List[Dict]:
    """
    Deserialize a batch of FileSets and run the processor.

    Called once per batch (one dynamically-mapped task instance). Exceptions during
    Processor construction are caught and returned as failure results for all files in
    the batch.

    :param serialized_file_sets: List of JSON-serialized FileSets forming one batch.
    :param config: Pipeline configuration.
    :param run_id: Unique run identifier (e.g. Airflow dag_run.run_id).
    :param start_date: Run start timestamp. Defaults to utcnow().
    :param kwargs: Extra keyword arguments forwarded to the processor constructor (e.g.
        extra_processor_init_kwargs).
    :return: List of dicts, one per FileSet, with keys "files", "success", and "error".
    """

    if start_date is None:
        start_date = datetime.now(tz=timezone.utc)
    if not run_id:
        run_id = start_date.strftime("%Y%m%dT%H%M%S")

    file_sets = [
        FileSet.from_serializable(s, config.file_types) for s in serialized_file_sets
    ]

    LOGGER.info(
        "Processing %d file sets in this batch.",
        len(file_sets),
    )

    try:
        processor = config.processor_class(
            config=config,
            run_id=run_id,
            start_date=start_date,
            file_sets=file_sets,
            **kwargs,
        )
        return processor.process()
    except Exception:
        # Constructor or init failure: all files fail.
        all_files = [f.name for fs in file_sets for f in fs.file_paths]
        return [
            {
                "files": all_files,
                "success": False,
                "error": format_exc(),
            }
        ]


def store(
    all_results: List[Any],
    config: ETLConfig,
    **kwargs: Any,
) -> Dict[str, int]:
    """
    Move processed files to store/ (success) or quarantine/ (failure).

    :param all_results: Nested list of dicts returned by process_wrapper across all
        batches. Automatically flattened.
    :param config: Pipeline configuration.
    :param kwargs: Extra keyword arguments forwarded by the orchestrator (e.g.
        extra_store_kwargs). Ignored by the default implementation but available to
        custom store_callable replacements.
    :return: Dict with counts {"stored": N, "quarantined": M}.
    """

    if config.store_callable:
        return config.store_callable(all_results, config, **kwargs)

    dirs = config.data_dirs

    # Check existence of target directories.
    for state in [
        DataState.PROCESS,
        DataState.STORE,
        DataState.QUARANTINE,
    ]:
        dir_path = getattr(dirs, state.value)
        if not dir_path.exists():
            raise FileNotFoundError(
                f"The '{state.value}' directory " f"does not exist."
            )

    # Flatten nested results from mapped/parallel tasks.
    flat_results: List[Dict] = []
    for item in all_results:
        if isinstance(item, list):
            flat_results.extend(item)
        else:
            flat_results.append(item)

    stored = quarantined = 0
    to_store: set = set()
    to_quarantine: set = set()

    for result in flat_results:
        target = DataState.STORE if result.get("success") else DataState.QUARANTINE
        for filename in result.get("files", []):
            src = dirs.process / filename
            if not src.exists():
                continue
            target_dir = getattr(dirs, target.value)
            shutil.move(str(src), str(target_dir / src.name))
            if target == DataState.STORE:
                stored += 1
                to_store.add(filename)
            else:
                quarantined += 1
                to_quarantine.add(filename)

    LOGGER.info(
        "Store results:\n"
        "Moved %d files to 'store': %s.\n"
        "Moved %d files to 'quarantine': %s.",
        stored,
        sorted(to_store),
        quarantined,
        sorted(to_quarantine),
    )

    return {"stored": stored, "quarantined": quarantined}


# ---------------------------------------------------------------
# Processor ABC: subclass in pipelines/{name}/process.py.
# ---------------------------------------------------------------


class Processor(ABC):
    """
    Abstract base class for pipeline processors.

    Implement process_file_set() with your domain logic. The process() template method
    handles DB session management, per-FileSet transactions, and error capture.
    """

    def __init__(
        self,
        config: ETLConfig,
        run_id: str,
        start_date: datetime,
        file_sets: List[FileSet],
        **kwargs: Any,
    ) -> None:
        """
        Initialize the processor.

        :param config: Pipeline configuration.
        :param run_id: Unique run identifier.
        :param start_date: Run start timestamp.
        :param file_sets: List of FileSets to process in this batch.
        :param kwargs: Extra keyword arguments from the orchestrator (e.g.
            extra_processor_init_kwargs).
        """

        self.config = config
        self.run_id = run_id
        self.start_date = start_date
        self.file_sets = file_sets
        self.extra_kwargs = kwargs

    def process(self) -> List[Dict[str, Any]]:
        """
        Template method: process each FileSet in its own session.

        Each FileSet gets an independent database transaction.
        Failures are captured per-FileSet without affecting
        others.

        :return: List of result dicts, one per FileSet. Each has
            keys "files", "success", and "error".
        """

        from lib.sql_utils import get_engine

        engine = get_engine(schema=self.config.db_schema)
        results: List[Dict[str, Any]] = []

        for file_set in self.file_sets:
            with Session(engine, autoflush=self.config.autoflush) as session:
                self.prepare_session(session=session)
                result = self._try_process_file_set(file_set=file_set, session=session)
                results.append(result)

        return results

    def prepare_session(self, session: Session) -> None:
        """
        Prepare the session before processing file sets.

        Override this method to add setup logic to the session (e.g. creating caches,
        executing setup queries).

        :param session: SQLAlchemy Session object.
        """

        return

    def _try_process_file_set(
        self, file_set: FileSet, session: Session
    ) -> Dict[str, Any]:
        """
        Attempt to process the file set and commit.

        Successes and failures are recorded in the returned result dict.

        :param file_set: FileSet to process.
        :param session: SQLAlchemy Session object.
        :return: Dict with keys "files", "success", and "error".
        """

        file_names = [f.name for f in file_set.file_paths]
        try:
            LOGGER.info("Processing file set: %s.", file_names)
            self.process_file_set(file_set=file_set, session=session)
            session.commit()
            LOGGER.info("File set processed successfully.")
            return {
                "files": file_names,
                "success": True,
                "error": None,
            }
        except Exception:
            LOGGER.error(format_exc())
            LOGGER.info("Rolling back database transaction.")
            session.rollback()
            return {
                "files": file_names,
                "success": False,
                "error": format_exc(),
            }

    @abstractmethod
    def process_file_set(self, file_set: FileSet, session: Session) -> None:
        """
        Domain logic: parse files, build ORM instances, upsert.

        Raise any exception to signal failure. The template
        method captures it and routes files to quarantine/.

        :param file_set: FileSet to process.
        :param session: SQLAlchemy Session object.
        """
        ...
