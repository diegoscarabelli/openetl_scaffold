"""File state machine: DataState, ETLDataDirectories, FileSet."""
from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional


class DataState(Enum):
    INGEST = "ingest"
    PROCESS = "process"
    STORE = "store"
    QUARANTINE = "quarantine"


@dataclass
class ETLDataDirectories:
    """Manages the four pipeline data directories.

    Base path comes from DATA_DIR env var (defaults to ./data relative to cwd).
    All four directories are created on __post_init__.
    """

    pipeline_id: str = ""
    ingest: Optional[Path] = field(init=False, default=None)
    process: Optional[Path] = field(init=False, default=None)
    store: Optional[Path] = field(init=False, default=None)
    quarantine: Optional[Path] = field(init=False, default=None)

    def __post_init__(self) -> None:
        if not self.pipeline_id:
            return
        data_root = Path(os.getenv("DATA_DIR", "data")).expanduser().resolve()
        base = data_root / self.pipeline_id
        self.ingest = base / DataState.INGEST.value
        self.process = base / DataState.PROCESS.value
        self.store = base / DataState.STORE.value
        self.quarantine = base / DataState.QUARANTINE.value
        for directory in (self.ingest, self.process, self.store, self.quarantine):
            directory.mkdir(parents=True, exist_ok=True)

    def move(self, src: Path, state: DataState) -> Path:
        """Move a file to the directory for state. Returns the new path."""
        target_dir: Path = getattr(self, state.value)
        dst = target_dir / src.name
        shutil.move(str(src), dst)
        return dst


@dataclass
class FileSet:
    """A logical group of related files within one processing batch.

    files maps file type enum name (str) to a list of Paths. Using the enum
    name (not the enum member) keeps FileSet JSON-serializable without custom
    encoding.
    """

    files: Dict[str, List[Path]]

    def get_files(self, file_type: Enum) -> List[Path]:
        """Return files for a given file type, or [] if none present."""
        return self.files.get(file_type.name, [])

    def all_files(self) -> List[Path]:
        """Flat list of all files across all types."""
        return [f for paths in self.files.values() for f in paths]

    def to_serializable(self) -> str:
        """Serialize to a JSON string for XCom / task result passing."""
        return json.dumps({k: [str(p) for p in v] for k, v in self.files.items()})

    @classmethod
    def from_serializable(cls, data: str) -> "FileSet":
        """Deserialize from the JSON string produced by to_serializable()."""
        raw = json.loads(data)
        return cls(files={k: [Path(p) for p in v] for k, v in raw.items()})
