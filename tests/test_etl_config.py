"""
Unit tests for lib.etl_config module.

This test suite covers:
    - ETLConfig dataclass initialization and validation.
    - Default value behavior for pipeline_print_name and db_schema.
    - __post_init__ validation of required fields.
    - String representation.
"""

import re
from enum import Enum

import pytest

from lib.etl_config import ETLConfig
from lib.filesystem_utils import DefaultFileTypes

# -----------------------------------------------------------------------
# TestETLConfig
# -----------------------------------------------------------------------


class TestETLConfig:
    """
    Tests for ETLConfig dataclass.
    """

    def test_defaults(self, tmp_path, monkeypatch) -> None:
        """
        Test default values for pipeline_print_name and db_schema.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        config = ETLConfig(pipeline_id="my_pipeline")
        assert config.pipeline_print_name == "my_pipeline"
        assert config.db_schema == "my_pipeline"
        assert config.max_process_tasks == 1
        assert config.min_file_sets_in_batch == 1
        assert config.autoflush is True
        assert config.file_types is DefaultFileTypes

    def test_explicit_values(self, tmp_path, monkeypatch) -> None:
        """
        Test that explicit values override defaults.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        config = ETLConfig(
            pipeline_id="test",
            pipeline_print_name="Test Pipeline",
            db_schema="custom_schema",
            max_process_tasks=4,
            min_file_sets_in_batch=2,
            autoflush=False,
        )
        assert config.pipeline_print_name == "Test Pipeline"
        assert config.db_schema == "custom_schema"
        assert config.max_process_tasks == 4
        assert config.min_file_sets_in_batch == 2
        assert config.autoflush is False

    def test_empty_pipeline_id_raises(self, tmp_path, monkeypatch) -> None:
        """
        Test that empty pipeline_id raises ValueError.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        with pytest.raises(ValueError, match="pipeline_id must not be empty"):
            ETLConfig(pipeline_id="")

    def test_invalid_max_process_tasks(self, tmp_path, monkeypatch) -> None:
        """
        Test that max_process_tasks < 1 raises ValueError.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        with pytest.raises(ValueError, match="max_process_tasks"):
            ETLConfig(pipeline_id="test", max_process_tasks=0)

    def test_invalid_min_file_sets_in_batch(self, tmp_path, monkeypatch) -> None:
        """
        Test that min_file_sets_in_batch < 1 raises ValueError.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        with pytest.raises(ValueError, match="min_file_sets_in_batch"):
            ETLConfig(pipeline_id="test", min_file_sets_in_batch=0)

    def test_non_enum_file_types_raises(self, tmp_path, monkeypatch) -> None:
        """
        Test that non-Enum file_types raises ValueError.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        with pytest.raises(ValueError, match="file_types"):
            ETLConfig(pipeline_id="test", file_types=str)

    def test_custom_file_types(self, tmp_path, monkeypatch) -> None:
        """
        Test that a custom Enum is accepted as file_types.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))

        class CustomTypes(Enum):
            CSV = re.compile(r".*\.csv$")

        config = ETLConfig(pipeline_id="test", file_types=CustomTypes)
        assert config.file_types is CustomTypes

    def test_data_dirs_set(self, tmp_path, monkeypatch) -> None:
        """
        Test that data_dirs paths are set during initialization.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        config = ETLConfig(pipeline_id="test_dirs")
        assert config.data_dirs.ingest is not None
        assert config.data_dirs.process is not None
        assert config.data_dirs.store is not None
        assert config.data_dirs.quarantine is not None
        assert "test_dirs" in str(config.data_dirs.ingest)

    def test_str(self, tmp_path, monkeypatch) -> None:
        """
        Test string representation of ETLConfig.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        config = ETLConfig(pipeline_id="my_pipe")
        result = str(config)
        assert "my_pipe" in result
        assert "ETLConfig" in result
