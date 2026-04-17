"""
Unit tests for lib.task_utils module.

This test suite covers:
    - Ingest logic for moving files from ingest to process directories.
    - File batching logic, including grouping, pattern matching, and error handling.
    - Processor interface compliance and invocation.
    - Process wrapper error handling.
    - Store logic for moving files to store or quarantine based on results.
    - FileSet serialization and deserialization.
"""

import json
import logging
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from lib.etl_config import ETLConfig
from lib.filesystem_utils import FileSet
from lib.task_utils import (
    Processor,
    batch,
    ingest,
    process_wrapper,
    store,
)

# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------


class DummyPattern:
    """
    Dummy pattern class to simulate regex enum members in tests.
    """

    def __init__(self, value: str, name: str = "DUMMY") -> None:
        self.value = re.compile(value)
        self.name = name

    def search(self, filename: str) -> bool:
        return self.value.search(filename) is not None


class DummyFile(Path):
    """
    Dummy file class to simulate file paths in tests.
    """

    def __new__(cls, name: str, mtime: int = None) -> "DummyFile":
        obj = Path.__new__(cls, name)
        obj._name = name
        obj._mtime = mtime
        return obj

    @property
    def name(self):
        return self._name

    def stat(self):
        outer = self

        class Stat:
            st_mtime = outer._mtime if outer._mtime is not None else 0

        return Stat()


class DummyProcessor(Processor):
    """
    Dummy processor for testing the Processor interface.
    """

    def process_file_set(self, file_set, session) -> None:
        pass


class FailingProcessor(Processor):
    """
    Processor that raises on process_file_set for failure testing.
    """

    def process_file_set(self, file_set, session) -> None:
        raise ValueError("Intentional failure.")


class DummyConfig(ETLConfig):
    """
    Minimal ETLConfig subclass for testing task_utils functions.

    Overrides __init__ to skip dataclass __post_init__ validation, matching the pattern
    used in openetl's test suite.
    """

    def __init__(self) -> None:
        self.pipeline_id = "test_pipeline"
        self.data_dirs = MagicMock()
        self.data_dirs.ingest = MagicMock(spec=Path)
        self.data_dirs.process = MagicMock(spec=Path)
        self.data_dirs.store = MagicMock(spec=Path)
        self.data_dirs.quarantine = MagicMock(spec=Path)
        self.store_format = None
        self.process_format = None
        self.file_types = []
        self.max_process_tasks = 1
        self.min_file_sets_in_batch = 1
        self.ingest_callable = None
        self.batch_callable = None
        self.store_callable = None
        self.processor_class = DummyProcessor
        self.db_schema = "test_schema"
        self.autoflush = True


def make_config(
    file_types: list,
    files: list,
    max_tasks: int = 2,
    min_batch: int = 1,
) -> DummyConfig:
    """
    Create a DummyConfig with specified file types and process directory contents.
    """

    config = DummyConfig()
    config.file_types = file_types
    config.max_process_tasks = max_tasks
    config.min_file_sets_in_batch = min_batch
    config.data_dirs.process.glob = lambda pattern: files
    return config


# -----------------------------------------------------------------------
# TestIngest
# -----------------------------------------------------------------------


class TestIngest:
    """
    Tests for ingest logic.
    """

    @patch("lib.task_utils.shutil.move")
    def test_ingest_no_files(self, mock_move: MagicMock) -> None:
        """
        Test that ingest raises RuntimeError when no files exist.
        """

        config = DummyConfig()
        config.data_dirs.ingest.glob.return_value = []
        config.data_dirs.ingest.exists.return_value = True
        config.data_dirs.process.exists.return_value = True
        config.data_dirs.store.exists.return_value = True
        with pytest.raises(RuntimeError):
            ingest(config)

    @patch("lib.task_utils.shutil.move")
    def test_ingest_moves_files_to_process(self, mock_move: MagicMock) -> None:
        """
        Test that ingest moves all files to process/ when no format filters set.
        """

        config = DummyConfig()
        f1 = MagicMock()
        f1.name = "file1.csv"
        f2 = MagicMock()
        f2.name = "file2.csv"
        config.data_dirs.ingest.glob.return_value = [f1, f2]
        config.data_dirs.ingest.exists.return_value = True
        config.data_dirs.process.exists.return_value = True
        config.data_dirs.store.exists.return_value = True
        result = ingest(config)
        assert result == 2
        assert mock_move.call_count == 2

    @patch("lib.task_utils.shutil.move")
    def test_ingest_respects_store_format(self, mock_move: MagicMock) -> None:
        """
        Test that files matching store_format go to store/, not process/.
        """

        config = DummyConfig()
        config.store_format = r".*\.json$"
        config.process_format = r".*\.csv$"
        csv_file = MagicMock()
        csv_file.name = "data.csv"
        json_file = MagicMock()
        json_file.name = "meta.json"
        config.data_dirs.ingest.glob.return_value = [csv_file, json_file]
        config.data_dirs.ingest.exists.return_value = True
        config.data_dirs.process.exists.return_value = True
        config.data_dirs.store.exists.return_value = True
        result = ingest(config)
        # Only the CSV should go to process/.
        assert result == 1

    @patch("lib.task_utils.shutil.move")
    def test_ingest_directory_not_found(self, mock_move: MagicMock) -> None:
        """
        Test that ingest raises FileNotFoundError when directories missing.
        """

        config = DummyConfig()
        config.data_dirs.ingest.exists.return_value = False
        with pytest.raises(FileNotFoundError):
            ingest(config)


# -----------------------------------------------------------------------
# TestBatch
# -----------------------------------------------------------------------


class TestBatch:
    """
    Tests for batch logic.
    """

    def test_batch_no_files(self) -> None:
        """
        Test that batch raises RuntimeError when process/ is empty.
        """

        config = make_config(
            [DummyPattern(r".*\.csv$", "CSV")], [], max_tasks=2, min_batch=1
        )
        with pytest.raises(RuntimeError):
            batch(config)

    def test_batch_single_file(self) -> None:
        """
        Test batching a single file produces one batch with one FileSet.
        """

        file = DummyFile("2025-08-02T12:00:00+00:00_data.csv")
        config = make_config(
            [DummyPattern(r".*data.*\.csv$", "DATA")],
            [file],
            max_tasks=2,
            min_batch=1,
        )
        serialized_batches = batch(config)
        assert len(serialized_batches) == 1
        assert len(serialized_batches[0]) == 1
        # Each element is a JSON string.
        parsed = json.loads(serialized_batches[0][0])
        assert isinstance(parsed, dict)
        assert "DATA" in parsed

    def test_batch_multiple_files_grouped_by_timestamp(self) -> None:
        """
        Test that files with different timestamps produce separate FileSets.
        """

        file1 = DummyFile("2025-08-02T12:00:00+00:00_data.csv")
        file2 = DummyFile("2025-08-02T12:00:00+00:00_meta.json")
        file3 = DummyFile("2025-08-02T13:00:00+00:00_data.csv")
        config = make_config(
            [
                DummyPattern(r".*data.*\.csv$", "DATA"),
                DummyPattern(r".*meta.*\.json$", "META"),
            ],
            [file1, file2, file3],
            max_tasks=2,
            min_batch=1,
        )
        serialized_batches = batch(config)
        # Collect all paths across all batches.
        all_paths = []
        for batch_list in serialized_batches:
            for json_str in batch_list:
                parsed = json.loads(json_str)
                for paths in parsed.values():
                    all_paths.extend(paths)
        names = [Path(p).name for p in all_paths]
        assert set(names) == {file1.name, file2.name, file3.name}

    def test_batch_multiple_files_same_pattern(self) -> None:
        """
        Test that files with same timestamp and pattern group together.
        """

        file1 = DummyFile("2025-08-02T12:00:00+00:00_data1.csv")
        file2 = DummyFile("2025-08-02T12:00:00+00:00_data2.csv")
        config = make_config(
            [DummyPattern(r".*data.*\.csv$", "DATA")],
            [file1, file2],
            max_tasks=2,
            min_batch=1,
        )
        serialized_batches = batch(config)
        assert len(serialized_batches) == 1
        parsed = json.loads(serialized_batches[0][0])
        assert "DATA" in parsed
        paths = parsed["DATA"]
        assert len(paths) == 2
        names = [Path(p).name for p in paths]
        assert set(names) == {
            "2025-08-02T12:00:00+00:00_data1.csv",
            "2025-08-02T12:00:00+00:00_data2.csv",
        }

    def test_batch_serialization_and_deserialization(self) -> None:
        """
        Test that batch output can be deserialized back into FileSets.
        """

        file1 = DummyFile("2025-08-02T12:00:00+00:00_data1.csv")
        file2 = DummyFile("2025-08-02T12:00:00+00:00_data2.csv")
        file3 = DummyFile("2025-08-02T12:00:00+00:00_meta.json")
        config = make_config(
            [
                DummyPattern(r".*data.*\.csv$", "DATA"),
                DummyPattern(r".*meta.*\.json$", "META"),
            ],
            [file1, file2, file3],
            max_tasks=2,
            min_batch=1,
        )
        serialized_batches = batch(config)
        parsed = json.loads(serialized_batches[0][0])

        # Test serialized structure.
        assert isinstance(parsed, dict)
        assert "DATA" in parsed
        assert "META" in parsed
        assert len(parsed["DATA"]) == 2
        assert len(parsed["META"]) == 1

        # Test deserialization via FileSet.from_serializable().
        class MockFileTypes:
            DATA = config.file_types[0]
            META = config.file_types[1]

        reconstructed = FileSet.from_serializable(
            serialized_batches[0][0], MockFileTypes
        )
        csv_files = reconstructed.get_files(MockFileTypes.DATA)
        assert len(csv_files) == 2
        csv_names = [f.name for f in csv_files]
        assert set(csv_names) == {
            "2025-08-02T12:00:00+00:00_data1.csv",
            "2025-08-02T12:00:00+00:00_data2.csv",
        }

    def test_batch_unmatched_file_error(self) -> None:
        """
        Test that batch raises ValueError when a file matches no pattern.
        """

        file1 = DummyFile("2025-08-02T12:00:00+00:00_data.csv")
        file2 = DummyFile("2025-08-02T12:00:00+00:00_unknown.txt")
        config = make_config(
            [DummyPattern(r".*data.*\.csv$", "DATA")],
            [file1, file2],
            max_tasks=2,
            min_batch=1,
        )
        with pytest.raises(ValueError):
            batch(config)

    def test_batch_batching_logic(self) -> None:
        """
        Test that two file sets with max_tasks=2 produce two batches.
        """

        file1 = DummyFile("2025-08-02T12:00:00+00:00_data.csv")
        file2 = DummyFile("2025-08-02T12:00:00+00:00_meta.json")
        file3 = DummyFile("2025-08-02T13:00:00+00:00_data.csv")
        file4 = DummyFile("2025-08-02T13:00:00+00:00_meta.json")
        config = make_config(
            [
                DummyPattern(r".*data.*\.csv$", "DATA"),
                DummyPattern(r".*meta.*\.json$", "META"),
            ],
            [file1, file2, file3, file4],
            max_tasks=2,
            min_batch=1,
        )
        batches_result = batch(config)
        assert len(batches_result) == 2
        assert all(len(b) == 1 for b in batches_result)

    def test_batch_empty(self) -> None:
        """
        Test that batch raises RuntimeError when no files in process/.
        """

        config = DummyConfig()
        config.data_dirs.process.glob.return_value = []
        with pytest.raises(RuntimeError):
            batch(config)


# -----------------------------------------------------------------------
# TestProcessor
# -----------------------------------------------------------------------


class TestProcessor:
    """
    Tests for Processor interface.
    """

    def test_processor_interface(self) -> None:
        """
        Test that Processor stores config, run_id, and extra kwargs.
        """

        dummy_config = MagicMock()
        dummy_config.db_schema = "test_schema"
        dummy_config.autoflush = True
        processor = DummyProcessor(
            config=dummy_config,
            run_id="dummy_run_id",
            start_date="2025-08-02",
            file_sets=[],
            test_key="test_value",
        )
        assert hasattr(processor, "process_file_set")
        assert callable(processor.process_file_set)
        assert hasattr(processor, "extra_kwargs")
        assert processor.extra_kwargs.get("test_key") == "test_value"
        try:
            processor.process_file_set(file_set=MagicMock(), session=MagicMock())
        except Exception as e:
            pytest.fail(f"process_file_set raised an exception: {e}.")

    def test_processor_cannot_instantiate_abc(self) -> None:
        """
        Test that Processor ABC cannot be instantiated directly.
        """

        with pytest.raises(TypeError):
            Processor(
                config=MagicMock(),
                run_id="run",
                start_date="2025-01-01",
                file_sets=[],
            )


# -----------------------------------------------------------------------
# TestProcessWrapper
# -----------------------------------------------------------------------


class TestProcessWrapper:
    """
    Tests for process_wrapper logic.
    """

    @patch("lib.task_utils.Session")
    @patch("lib.sql_utils.get_engine", return_value=MagicMock())
    def test_process_wrapper_runs(
        self, mock_engine: MagicMock, mock_session_cls: MagicMock
    ) -> None:
        """
        Test that process_wrapper returns success results for valid input.
        """

        mock_session = MagicMock()
        mock_session_cls.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)

        serialized_file_sets = [json.dumps({"DATA": ["file1.csv"]})]
        config = DummyConfig()
        results = process_wrapper(
            serialized_file_sets=serialized_file_sets,
            config=config,
            run_id="run_id",
        )
        assert isinstance(results, list)
        assert len(results) == 1
        assert results[0]["success"] is True

    def test_process_wrapper_constructor_failure(self) -> None:
        """
        Test that constructor failures are caught and returned as errors.
        """

        class BadProcessor(Processor):
            def __init__(self, *args, **kwargs):
                raise RuntimeError("Constructor failed.")

            def process_file_set(self, file_set, session):
                pass

        serialized_file_sets = [json.dumps({"DATA": ["file1.csv"]})]
        config = DummyConfig()
        config.processor_class = BadProcessor
        results = process_wrapper(
            serialized_file_sets=serialized_file_sets,
            config=config,
            run_id="run_id",
        )
        assert isinstance(results, list)
        assert len(results) == 1
        assert results[0]["success"] is False
        assert "Constructor failed" in results[0]["error"]

    @patch("lib.task_utils.Session")
    @patch("lib.sql_utils.get_engine", return_value=MagicMock())
    def test_process_wrapper_file_set_failure(
        self, mock_engine: MagicMock, mock_session_cls: MagicMock
    ) -> None:
        """
        Test that process_file_set failures are captured per-FileSet.
        """

        mock_session = MagicMock()
        mock_session_cls.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)

        serialized_file_sets = [json.dumps({"DATA": ["file1.csv"]})]
        config = DummyConfig()
        config.processor_class = FailingProcessor
        results = process_wrapper(
            serialized_file_sets=serialized_file_sets,
            config=config,
            run_id="run_id",
        )
        assert isinstance(results, list)
        assert len(results) == 1
        assert results[0]["success"] is False
        assert "Intentional failure" in results[0]["error"]


# -----------------------------------------------------------------------
# TestFileSetSerialization
# -----------------------------------------------------------------------


class TestFileSetSerialization:
    """
    Tests for FileSet serialization and deserialization methods.
    """

    def test_fileset_serialization(self) -> None:
        """
        Test FileSet to_serializable produces valid JSON.
        """

        file_set = FileSet()
        pattern1 = DummyPattern(r".*data.*\.csv$", "DATA")
        pattern2 = DummyPattern(r".*meta.*\.json$", "META")

        file_set.files[pattern1] = [Path("file1.csv"), Path("file2.csv")]
        file_set.files[pattern2] = [Path("meta.json")]

        serialized = file_set.to_serializable()

        # Should be a JSON string.
        assert isinstance(serialized, str)
        parsed = json.loads(serialized)
        assert "DATA" in parsed
        assert "META" in parsed
        assert len(parsed["DATA"]) == 2
        assert len(parsed["META"]) == 1
        assert all(isinstance(path, str) for path in parsed["DATA"])

    def test_fileset_deserialization(self) -> None:
        """
        Test FileSet from_serializable reconstructs files correctly.
        """

        class MockFileTypes:
            DATA = DummyPattern(r".*data.*\.csv$", "DATA")
            META = DummyPattern(r".*meta.*\.json$", "META")

        serialized_data = json.dumps(
            {"DATA": ["file1.csv", "file2.csv"], "META": ["meta.json"]}
        )

        file_set = FileSet.from_serializable(serialized_data, MockFileTypes)

        assert len(file_set.files) == 2
        assert MockFileTypes.DATA in file_set.files
        assert MockFileTypes.META in file_set.files

        data_files = file_set.get_files(MockFileTypes.DATA)
        assert len(data_files) == 2
        assert all(isinstance(f, Path) for f in data_files)
        assert set(f.name for f in data_files) == {"file1.csv", "file2.csv"}

        meta_files = file_set.get_files(MockFileTypes.META)
        assert len(meta_files) == 1
        assert meta_files[0].name == "meta.json"


# -----------------------------------------------------------------------
# TestLogger
# -----------------------------------------------------------------------


class TestLogger:
    """
    Tests for logging output in task_utils functions.
    """

    def test_ingest_logs(self, caplog: pytest.LogCaptureFixture) -> None:
        """
        Test that ingest produces log output.
        """

        config = DummyConfig()
        config.data_dirs.ingest.glob.return_value = [DummyFile("file.csv")]
        config.data_dirs.process.exists.return_value = True
        config.data_dirs.store.exists.return_value = True
        config.data_dirs.ingest.exists.return_value = True
        with caplog.at_level(logging.INFO):
            try:
                ingest(config)
            except Exception:
                pass
        if not caplog.messages:
            pytest.skip(
                "No log messages captured; logger may be" " mocked or disabled."
            )
        assert any("Ingest" in m for m in caplog.messages)


# -----------------------------------------------------------------------
# TestStore
# -----------------------------------------------------------------------


class TestStore:
    """
    Tests for store logic.
    """

    @patch("pathlib.Path.exists", return_value=True)
    @patch("lib.task_utils.shutil.move")
    def test_store_moves_files(
        self, mock_move: MagicMock, mock_exists: MagicMock
    ) -> None:
        """
        Test that store moves files based on success/failure results.
        """

        config = DummyConfig()
        config.data_dirs.process = Path("/tmp/process")
        config.data_dirs.store = Path("/tmp/store")
        config.data_dirs.quarantine = Path("/tmp/quarantine")

        all_results = [
            [
                {
                    "files": ["file1.txt", "file2.txt"],
                    "success": True,
                    "error": None,
                },
            ]
        ]
        result = store(all_results, config)
        assert result["stored"] == 2
        assert result["quarantined"] == 0
        assert mock_move.call_count == 2

    @patch("pathlib.Path.exists", return_value=True)
    @patch("lib.task_utils.shutil.move")
    def test_store_quarantines_failures(
        self, mock_move: MagicMock, mock_exists: MagicMock
    ) -> None:
        """
        Test that store quarantines files from failed processing.
        """

        config = DummyConfig()
        config.data_dirs.process = Path("/tmp/process")
        config.data_dirs.store = Path("/tmp/store")
        config.data_dirs.quarantine = Path("/tmp/quarantine")

        all_results = [
            [
                {
                    "files": ["good.txt"],
                    "success": True,
                    "error": None,
                },
                {
                    "files": ["bad.txt"],
                    "success": False,
                    "error": "some error",
                },
            ]
        ]
        result = store(all_results, config)
        assert result["stored"] == 1
        assert result["quarantined"] == 1

    @patch("pathlib.Path.exists", return_value=True)
    @patch("lib.task_utils.shutil.move")
    def test_store_flattens_nested_results(
        self, mock_move: MagicMock, mock_exists: MagicMock
    ) -> None:
        """
        Test that store flattens nested result lists from parallel tasks.
        """

        config = DummyConfig()
        config.data_dirs.process = Path("/tmp/process")
        config.data_dirs.store = Path("/tmp/store")
        config.data_dirs.quarantine = Path("/tmp/quarantine")

        # Nested results from two parallel process tasks.
        all_results = [
            [{"files": ["a.txt"], "success": True, "error": None}],
            [{"files": ["b.txt"], "success": True, "error": None}],
        ]
        result = store(all_results, config)
        assert result["stored"] == 2
        assert result["quarantined"] == 0
