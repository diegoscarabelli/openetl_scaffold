"""
Unit tests for lib.filesystem_utils module.

This test suite covers:
    - DataState enum values.
    - ETLDataDirectories path construction and directory creation.
    - DATA_DIR environment variable fallback behavior.
    - FileSet operations: file_paths, get_files, serialization, get_total_size.
    - DefaultFileTypes catch-all pattern.
"""

import re
from enum import Enum
from pathlib import Path


from lib.filesystem_utils import (
    DataState,
    DefaultFileTypes,
    ETLDataDirectories,
    FileSet,
)

# -----------------------------------------------------------------------
# TestDataState
# -----------------------------------------------------------------------


class TestDataState:
    """
    Tests for DataState enum.
    """

    def test_data_state_values(self) -> None:
        """
        Test that DataState has all expected values.
        """

        assert DataState.INGEST.value == "ingest"
        assert DataState.PROCESS.value == "process"
        assert DataState.QUARANTINE.value == "quarantine"
        assert DataState.STORE.value == "store"

    def test_data_state_count(self) -> None:
        """
        Test that DataState has exactly four members.
        """

        assert len(DataState) == 4


# -----------------------------------------------------------------------
# TestETLDataDirectories
# -----------------------------------------------------------------------


class TestETLDataDirectories:
    """
    Tests for ETLDataDirectories dataclass.
    """

    def test_set_paths_creates_directories(self, tmp_path, monkeypatch) -> None:
        """
        Test that set_paths creates all four directories.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        dirs = ETLDataDirectories()
        dirs.set_paths("test_pipeline")

        assert dirs.ingest is not None
        assert dirs.process is not None
        assert dirs.store is not None
        assert dirs.quarantine is not None

        # Directories should exist.
        assert dirs.ingest.is_dir()
        assert dirs.process.is_dir()
        assert dirs.store.is_dir()
        assert dirs.quarantine.is_dir()

    def test_set_paths_without_create(self, tmp_path, monkeypatch) -> None:
        """
        Test that set_paths with create_dirs=False skips directory creation.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        dirs = ETLDataDirectories()
        dirs.set_paths("no_create", create_dirs=False)

        assert dirs.ingest is not None
        # Directories should NOT exist.
        assert not dirs.ingest.exists()

    def test_path_structure(self, tmp_path, monkeypatch) -> None:
        """
        Test that directory paths follow DATA_DIR/pipeline/state structure.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        dirs = ETLDataDirectories()
        dirs.set_paths("my_pipe")

        assert dirs.ingest == tmp_path / "my_pipe" / "ingest"
        assert dirs.process == tmp_path / "my_pipe" / "process"
        assert dirs.store == tmp_path / "my_pipe" / "store"
        assert dirs.quarantine == tmp_path / "my_pipe" / "quarantine"

    def test_data_dir_fallback(self, monkeypatch) -> None:
        """
        Test that missing DATA_DIR falls back to 'data' directory.
        """

        monkeypatch.delenv("DATA_DIR", raising=False)
        dirs = ETLDataDirectories()
        dirs.set_paths("fallback", create_dirs=False)

        assert dirs.ingest == Path("data") / "fallback" / "ingest"


# -----------------------------------------------------------------------
# TestDefaultFileTypes
# -----------------------------------------------------------------------


class TestDefaultFileTypes:
    """
    Tests for DefaultFileTypes enum.
    """

    def test_default_pattern_matches_anything(self) -> None:
        """
        Test that DefaultFileTypes.DEFAULT matches any filename.
        """

        assert DefaultFileTypes.DEFAULT.value.match("anything.csv")
        assert DefaultFileTypes.DEFAULT.value.match("file.json")
        assert DefaultFileTypes.DEFAULT.value.match("")


# -----------------------------------------------------------------------
# TestFileSet
# -----------------------------------------------------------------------


class TestFileSet:
    """
    Tests for FileSet dataclass.
    """

    def test_file_paths_flat(self) -> None:
        """
        Test that file_paths returns a flat list from all file types.
        """

        class FT(Enum):
            A = re.compile(r".*\.csv$")
            B = re.compile(r".*\.json$")

        fs = FileSet(
            files={
                FT.A: [Path("a.csv"), Path("b.csv")],
                FT.B: [Path("c.json")],
            }
        )
        assert len(fs.file_paths) == 3

    def test_get_files_existing_type(self) -> None:
        """
        Test that get_files returns paths for a known file type.
        """

        fs = FileSet(files={DefaultFileTypes.DEFAULT: [Path("file.txt")]})
        result = fs.get_files(DefaultFileTypes.DEFAULT)
        assert len(result) == 1
        assert result[0] == Path("file.txt")

    def test_get_files_missing_type(self) -> None:
        """
        Test that get_files returns empty list for unknown type.
        """

        class FT(Enum):
            MISSING = re.compile(r".*")

        fs = FileSet()
        assert fs.get_files(FT.MISSING) == []

    def test_serialization_roundtrip(self) -> None:
        """
        Test that to_serializable and from_serializable round-trip.
        """

        fs = FileSet(
            files={
                DefaultFileTypes.DEFAULT: [
                    Path("/tmp/a.csv"),
                    Path("/tmp/b.csv"),
                ]
            }
        )
        serialized = fs.to_serializable()
        restored = FileSet.from_serializable(serialized, DefaultFileTypes)
        assert len(restored.file_paths) == 2
        assert restored.file_paths[0] == Path("/tmp/a.csv")

    def test_from_serializable_ignores_unknown_types(self) -> None:
        """
        Test that from_serializable skips unknown file type names.
        """

        import json

        data = json.dumps({"UNKNOWN_TYPE": ["/tmp/file.csv"]})
        fs = FileSet.from_serializable(data, DefaultFileTypes)
        assert len(fs.file_paths) == 0

    def test_get_total_size(self, tmp_path) -> None:
        """
        Test that get_total_size returns the sum of all file sizes.
        """

        file_a = tmp_path / "a.txt"
        file_b = tmp_path / "b.txt"
        file_a.write_text("hello")
        file_b.write_text("world!")

        fs = FileSet(files={DefaultFileTypes.DEFAULT: [file_a, file_b]})
        assert fs.get_total_size() == len("hello") + len("world!")

    def test_get_total_size_missing_file(self, tmp_path) -> None:
        """
        Test that get_total_size skips non-existent files.
        """

        fs = FileSet(files={DefaultFileTypes.DEFAULT: [tmp_path / "nonexistent.csv"]})
        assert fs.get_total_size() == 0
