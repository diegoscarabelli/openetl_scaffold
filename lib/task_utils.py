"""Standard task implementations: ingest, batch, process_wrapper, store.

Pure Python — no orchestrator imports. Wired into DAGs/flows via
lib/airflow_utils.py or lib/prefect_utils.py.
"""
from __future__ import annotations

import re
import shutil
from datetime import datetime
from traceback import format_exc
from typing import TYPE_CHECKING, Dict, List, Optional

from lib.filesystem_utils import DataState, FileSet

if TYPE_CHECKING:
    from lib.pipeline_config import PipelineConfig


def ingest(config: "PipelineConfig") -> int:
    """Scan ingest/ and route files to process/ or store/.

    Files matching process_format go to process/ for transformation.
    Files matching store_format bypass processing and go directly to store/.

    Raises RuntimeError when no matching files are found. Callers should
    translate this to the orchestrator's skip/cancel signal:
      - Airflow 3: raise AirflowSkipException
      - Prefect:   raise Abort or return a sentinel value

    Returns the number of files routed to process/.
    """
    dirs = config.data_dirs
    process_re = re.compile(config.process_format) if config.process_format else None
    store_re = re.compile(config.store_format) if config.store_format else None

    process_count = 0
    store_count = 0

    for f in sorted(dirs.ingest.iterdir()):
        if not f.is_file():
            continue
        if process_re and process_re.search(f.name):
            shutil.move(str(f), dirs.process / f.name)
            process_count += 1
        elif store_re and store_re.search(f.name):
            shutil.move(str(f), dirs.store / f.name)
            store_count += 1

    if process_count == 0 and store_count == 0:
        raise RuntimeError(
            f"[{config.pipeline_id}] No matching files in ingest/. "
            "Drop files matching process_format or store_format to trigger."
        )

    return process_count


def batch(config: "PipelineConfig") -> List[str]:
    """Group files in process/ into FileSet objects.

    Default: one FileSet per file. Override via config.batch_callable for
    more complex grouping (e.g. group by date prefix or user ID).

    Returns a list of JSON strings (one per FileSet) for XCom / task.map.
    """
    if config.batch_callable:
        return config.batch_callable(config)

    dirs = config.data_dirs
    file_types = list(config.file_types)
    results: List[str] = []

    for f in sorted(dirs.process.iterdir()):
        if not f.is_file():
            continue
        for ft in file_types:
            if ft.value.search(f.name):
                fs = FileSet(files={ft.name: [f]})
                results.append(fs.to_serializable())
                break

    return results


def process_wrapper(
    serialized_batch: str,
    config: "PipelineConfig",
    run_id: str = "",
    start_date: Optional[datetime] = None,
) -> Dict:
    """Deserialize a batch and run the processor.

    Called once per batch (one dynamically-mapped task instance).
    Returns {"files": [...], "success": bool, "error": str|None}.
    Never raises — errors are captured and returned for the store task.
    """
    if start_date is None:
        start_date = datetime.utcnow()
    if not run_id:
        run_id = start_date.strftime("%Y%m%dT%H%M%S")

    file_set = FileSet.from_serializable(serialized_batch)
    file_names = [f.name for f in file_set.all_files()]

    try:
        processor = config.processor_class(
            config=config,
            run_id=run_id,
            start_date=start_date,
            file_set=file_set,
        )
        result = processor.process()
        return {"files": file_names, **result}
    except Exception:
        return {"files": file_names, "success": False, "error": format_exc()}


def store(all_results: List[Dict], config: "PipelineConfig") -> Dict[str, int]:
    """Move processed files to store/ (success) or quarantine/ (failure).

    all_results: list of dicts returned by process_wrapper across all batches.
    Returns {"stored": N, "quarantined": M}.
    """
    if config.store_callable:
        return config.store_callable(all_results, config)

    dirs = config.data_dirs
    stored = quarantined = 0

    for batch_result in all_results:
        target = DataState.STORE if batch_result.get("success") else DataState.QUARANTINE
        for filename in batch_result.get("files", []):
            src = dirs.process / filename
            if not src.exists():
                continue
            dirs.move(src, target)
            if target == DataState.STORE:
                stored += 1
            else:
                quarantined += 1

    return {"stored": stored, "quarantined": quarantined}
