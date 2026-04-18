"""
Unit tests for lib.prefect_utils module.

This test suite covers:
    - PrefectETLConfig dataclass initialization and defaults.
    - create_standard_flow() flow assembly.
    - Flow early exit when ingest returns 0.
"""

from lib.prefect_utils import PrefectETLConfig, create_standard_flow

# -----------------------------------------------------------------------
# TestPrefectETLConfig
# -----------------------------------------------------------------------


class TestPrefectETLConfig:
    """
    Tests for PrefectETLConfig dataclass.
    """

    def test_defaults(self, tmp_path, monkeypatch) -> None:
        """
        Test default values for PrefectETLConfig.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        config = PrefectETLConfig(pipeline_id="test_flow")
        assert config.flow_retries == 0
        assert config.flow_retry_delay_seconds == 0
        assert config.flow_timeout_seconds is None
        assert config.task_retries == 0
        assert config.task_retry_delay_seconds == 0
        assert config.task_timeout_seconds is None

    def test_explicit_values(self, tmp_path, monkeypatch) -> None:
        """
        Test that explicit values override defaults.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        config = PrefectETLConfig(
            pipeline_id="explicit",
            flow_retries=3,
            flow_retry_delay_seconds=60,
            flow_timeout_seconds=3600,
            task_retries=2,
            task_retry_delay_seconds=30,
            task_timeout_seconds=600,
        )
        assert config.flow_retries == 3
        assert config.flow_timeout_seconds == 3600
        assert config.task_retries == 2

    def test_inherits_etl_config(self, tmp_path, monkeypatch) -> None:
        """
        Test that PrefectETLConfig inherits ETLConfig fields.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        config = PrefectETLConfig(
            pipeline_id="inherit_test",
            max_process_tasks=8,
            description="Test flow.",
        )
        assert config.max_process_tasks == 8
        assert config.pipeline_print_name == "inherit_test"
        assert config.db_schema == "inherit_test"
        assert config.description == "Test flow."

    def test_str(self, tmp_path, monkeypatch) -> None:
        """
        Test string representation.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        config = PrefectETLConfig(pipeline_id="str_test")
        result = str(config)
        assert "PrefectETLConfig" in result
        assert "str_test" in result


# -----------------------------------------------------------------------
# TestCreateStandardFlow
# -----------------------------------------------------------------------


class TestCreateStandardFlow:
    """
    Tests for create_standard_flow() flow factory.
    """

    def test_create_standard_flow_returns_callable(self, tmp_path, monkeypatch) -> None:
        """
        Test that create_standard_flow returns a callable flow.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        config = PrefectETLConfig(pipeline_id="flow_test")
        flow_fn = create_standard_flow(config)
        assert callable(flow_fn)

    def test_create_standard_flow_name(self, tmp_path, monkeypatch) -> None:
        """
        Test that the flow has the correct name.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        config = PrefectETLConfig(pipeline_id="named_flow")
        flow_fn = create_standard_flow(config)
        assert flow_fn.name == "named_flow"

    def test_create_standard_flow_with_extract(self, tmp_path, monkeypatch) -> None:
        """
        Test that create_standard_flow accepts an extract callable.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))

        def mock_extract(ingest_dir, db_schema):
            pass

        config = PrefectETLConfig(pipeline_id="extract_test")
        flow_fn = create_standard_flow(
            config,
            extract_callable=mock_extract,
            extract_kwargs={
                "ingest_dir": str(tmp_path),
                "db_schema": "test",
            },
        )
        assert callable(flow_fn)

    def test_create_standard_flow_without_extract(self, tmp_path, monkeypatch) -> None:
        """
        Test that create_standard_flow works without extract callable.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        config = PrefectETLConfig(pipeline_id="no_extract")
        flow_fn = create_standard_flow(config)
        assert callable(flow_fn)

    def test_flow_early_exit_no_files(self, tmp_path, monkeypatch) -> None:
        """
        Test that the flow exits early when ingest returns 0.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))

        def no_files_ingest(config, **kwargs):
            raise RuntimeError("No files.")

        config = PrefectETLConfig(
            pipeline_id="early_exit",
            ingest_callable=no_files_ingest,
        )
        flow_fn = create_standard_flow(config)
        # Flow should complete without error when ingest
        # raises RuntimeError (returns 0, exits early).
        flow_fn()
