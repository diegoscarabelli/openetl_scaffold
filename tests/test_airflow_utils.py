"""
Unit tests for lib.airflow_utils module.

This test suite covers:
    - AirflowETLConfig dataclass initialization and defaults.
    - dag_id and dag_args properties.
    - create_dag() DAG assembly and task wiring.
    - Ingest skip behavior on RuntimeError.
"""

from datetime import timedelta

import pytest

from lib.airflow_utils import AirflowETLConfig, create_dag

# -----------------------------------------------------------------------
# TestAirflowETLConfig
# -----------------------------------------------------------------------


class TestAirflowETLConfig:
    """
    Tests for AirflowETLConfig dataclass.
    """

    def test_defaults(self, tmp_path, monkeypatch) -> None:
        """
        Test default values for AirflowETLConfig.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        config = AirflowETLConfig(pipeline_id="test_pipe")
        assert config.dag_catchup is False
        assert config.dag_max_active_runs == 1
        assert config.dag_schedule_interval is None
        assert config.dag_dagrun_timeout == timedelta(hours=12)
        assert config.task_execution_timeout == timedelta(hours=1)
        assert config.dag_on_failure_callback is None
        assert config.extra_ingest_kwargs == {}
        assert config.extra_batch_kwargs == {}
        assert config.extra_processor_init_kwargs == {}
        assert config.extra_store_kwargs == {}

    def test_postgres_user_default(self, tmp_path, monkeypatch) -> None:
        """
        Test that postgres_user defaults to airflow_<pipeline_id>.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        config = AirflowETLConfig(pipeline_id="my_pipe")
        assert config.postgres_user == "airflow_my_pipe"

    def test_postgres_user_explicit(self, tmp_path, monkeypatch) -> None:
        """
        Test that explicit postgres_user is preserved.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        config = AirflowETLConfig(pipeline_id="my_pipe", postgres_user="custom_user")
        assert config.postgres_user == "custom_user"

    def test_dag_id_property(self, tmp_path, monkeypatch) -> None:
        """
        Test that dag_id is an alias for pipeline_id.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        config = AirflowETLConfig(pipeline_id="my_dag")
        assert config.dag_id == "my_dag"

    def test_dag_args_property(self, tmp_path, monkeypatch) -> None:
        """
        Test that dag_args returns correct dict structure.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        config = AirflowETLConfig(
            pipeline_id="args_test",
            description="A test DAG.",
            dag_catchup=True,
            dag_max_active_runs=3,
        )
        args = config.dag_args
        assert args["dag_id"] == "args_test"
        assert args["catchup"] is True
        assert args["max_active_runs"] == 3
        assert args["description"] == "A test DAG."
        assert args["doc_md"] == "A test DAG."
        assert "default_args" in args
        assert "on_failure_callback" in args["default_args"]
        assert "execution_timeout" in args["default_args"]

    def test_inherits_etl_config(self, tmp_path, monkeypatch) -> None:
        """
        Test that AirflowETLConfig inherits ETLConfig fields.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        config = AirflowETLConfig(pipeline_id="inherit_test", max_process_tasks=4)
        assert config.max_process_tasks == 4
        assert config.pipeline_print_name == "inherit_test"
        assert config.db_schema == "inherit_test"

    def test_str(self, tmp_path, monkeypatch) -> None:
        """
        Test string representation.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        config = AirflowETLConfig(pipeline_id="str_test")
        result = str(config)
        assert "AirflowETLConfig" in result
        assert "str_test" in result


# -----------------------------------------------------------------------
# TestCreateDag
# -----------------------------------------------------------------------


class TestCreateDag:
    """
    Tests for create_dag() DAG factory.
    """

    def test_create_dag_returns_dag(self, tmp_path, monkeypatch) -> None:
        """
        Test that create_dag returns an Airflow DAG object.
        """

        from airflow import DAG

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        config = AirflowETLConfig(pipeline_id="dag_test")
        dag = create_dag(config)
        assert isinstance(dag, DAG)
        assert dag.dag_id == "dag_test"

    def test_dag_has_four_tasks(self, tmp_path, monkeypatch) -> None:
        """
        Test that the DAG contains exactly four tasks.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        config = AirflowETLConfig(pipeline_id="task_count")
        dag = create_dag(config)
        assert len(dag.tasks) == 4

    def test_dag_task_ids(self, tmp_path, monkeypatch) -> None:
        """
        Test that the DAG has the expected task IDs.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        config = AirflowETLConfig(pipeline_id="task_ids")
        dag = create_dag(config)
        task_ids = {t.task_id for t in dag.tasks}
        assert task_ids == {"ingest", "batch", "process", "store"}

    def test_dag_task_dependencies(self, tmp_path, monkeypatch) -> None:
        """
        Test that tasks are wired ingest >> batch >> process >> store.
        """

        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        config = AirflowETLConfig(pipeline_id="deps_test")
        dag = create_dag(config)
        tasks = {t.task_id: t for t in dag.tasks}

        assert "batch" in [t.task_id for t in tasks["ingest"].downstream_list]
        assert "process" in [t.task_id for t in tasks["batch"].downstream_list]
        assert "store" in [t.task_id for t in tasks["process"].downstream_list]

    def test_ingest_skip_on_runtime_error(self, tmp_path, monkeypatch) -> None:
        """
        Test that ingest converts RuntimeError to AirflowSkipException.
        """

        from airflow.sdk.exceptions import AirflowSkipException

        monkeypatch.setenv("DATA_DIR", str(tmp_path))

        def failing_ingest(config, **kwargs):
            raise RuntimeError("No files.")

        config = AirflowETLConfig(
            pipeline_id="skip_test", ingest_callable=failing_ingest
        )
        dag = create_dag(config)
        ingest_task = dag.get_task("ingest")

        with pytest.raises(AirflowSkipException):
            ingest_task.python_callable(config=config)
