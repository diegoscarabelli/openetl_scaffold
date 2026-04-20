"""
Unit tests for pipelines.example.process module.

This test suite covers:
    - WIDProcessor.process_file_set() orchestration.
    - _seed_percentiles() idempotent dimension seeding.
    - _process_variable_meta() metadata parsing and upsert.
    - _process_countries() country parsing and upsert.
    - _process_observations() observation and data quality upsert.
    - _read_json() file reading utility.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from lib.filesystem_utils import FileSet
from pipelines.example.constants import PERCENTILES, WIDFileTypes
from pipelines.example.process import WIDProcessor, _read_json
from pipelines.example.sqla_models import (
    Country,
    DataQuality,
    Observation,
    Percentile,
    Variable,
)

# -----------------------------------------------------------------------
# TestReadJson
# -----------------------------------------------------------------------


class TestReadJson:
    """
    Tests for _read_json() file reading utility.
    """

    def test_reads_list(self, tmp_path: Path) -> None:
        """
        Test reading a JSON array file.
        """

        path = tmp_path / "data.json"
        path.write_text(json.dumps([{"a": 1}, {"b": 2}]))
        result = _read_json(path)
        assert result == [{"a": 1}, {"b": 2}]

    def test_reads_dict(self, tmp_path: Path) -> None:
        """
        Test reading a JSON object file.
        """

        path = tmp_path / "data.json"
        path.write_text(json.dumps({"key": "value"}))
        result = _read_json(path)
        assert result == {"key": "value"}


# -----------------------------------------------------------------------
# TestSeedPercentiles
# -----------------------------------------------------------------------


class TestSeedPercentiles:
    """
    Tests for WIDProcessor._seed_percentiles().
    """

    @patch("pipelines.example.process.upsert_model_instances")
    def test_creates_percentile_instances(self, mock_upsert: MagicMock) -> None:
        """
        Test that all percentile constants are seeded.
        """

        processor = WIDProcessor.__new__(WIDProcessor)
        session = MagicMock()

        processor._seed_percentiles(session)

        mock_upsert.assert_called_once()
        kwargs = mock_upsert.call_args[1]
        assert kwargs["session"] is session
        assert len(kwargs["model_instances"]) == len(PERCENTILES)
        assert kwargs["conflict_columns"] == ["percentile_code"]
        assert kwargs["on_conflict_update"] is False

    @patch("pipelines.example.process.upsert_model_instances")
    def test_percentile_instance_types(self, mock_upsert: MagicMock) -> None:
        """
        Test that instances are Percentile ORM objects.
        """

        processor = WIDProcessor.__new__(WIDProcessor)
        session = MagicMock()

        processor._seed_percentiles(session)

        instances = mock_upsert.call_args[1]["model_instances"]
        assert all(isinstance(i, Percentile) for i in instances)

    @patch("pipelines.example.process.upsert_model_instances")
    def test_percentile_values_match_constants(self, mock_upsert: MagicMock) -> None:
        """
        Test that seeded percentile values match the constant data.
        """

        processor = WIDProcessor.__new__(WIDProcessor)
        session = MagicMock()

        processor._seed_percentiles(session)

        instances = mock_upsert.call_args[1]["model_instances"]
        by_code = {i.percentile_code: i for i in instances}
        p0p10 = by_code["p0p10"]
        assert p0p10.lower_bound == 0
        assert p0p10.upper_bound == 10
        assert p0p10.width == 10
        assert p0p10.granularity == "decile"


# -----------------------------------------------------------------------
# TestProcessVariableMeta
# -----------------------------------------------------------------------


class TestProcessVariableMeta:
    """
    Tests for WIDProcessor._process_variable_meta().
    """

    @patch("pipelines.example.process.upsert_model_instances")
    def test_upserts_variable_records(
        self, mock_upsert: MagicMock, tmp_path: Path
    ) -> None:
        """
        Test that variable metadata records are upserted.
        """

        path = tmp_path / "variable_metadata_2024-01-01T00:00:00Z.json"
        path.write_text(
            json.dumps(
                [
                    {
                        "variable_code": "sptinc",
                        "concept": "ptinc",
                        "series_type": "s",
                        "short_name": "Income share",
                        "description": "Desc.",
                        "technical_description": "Tech.",
                        "unit": "share",
                        "population_type": "Adults",
                        "age_group": "20+",
                    }
                ]
            )
        )

        processor = WIDProcessor.__new__(WIDProcessor)
        session = MagicMock()
        processor._process_variable_meta(path, session)

        mock_upsert.assert_called_once()
        kwargs = mock_upsert.call_args[1]
        assert kwargs["conflict_columns"] == ["variable_code"]
        assert kwargs["on_conflict_update"] is True
        assert "short_name" in kwargs["update_columns"]
        assert "description" in kwargs["update_columns"]

    @patch("pipelines.example.process.upsert_model_instances")
    def test_variable_instance_fields(
        self, mock_upsert: MagicMock, tmp_path: Path
    ) -> None:
        """
        Test that Variable instances have correct field values.
        """

        path = tmp_path / "variable_metadata_2024-01-01T00:00:00Z.json"
        path.write_text(
            json.dumps(
                [
                    {
                        "variable_code": "shweal",
                        "concept": "hweal",
                        "series_type": "s",
                        "short_name": "Wealth share",
                        "unit": "share",
                    }
                ]
            )
        )

        processor = WIDProcessor.__new__(WIDProcessor)
        session = MagicMock()
        processor._process_variable_meta(path, session)

        instances = mock_upsert.call_args[1]["model_instances"]
        assert len(instances) == 1
        assert isinstance(instances[0], Variable)
        assert instances[0].variable_code == "shweal"
        assert instances[0].concept == "hweal"
        assert instances[0].series_type == "s"
        assert instances[0].short_name == "Wealth share"

    @patch("pipelines.example.process.upsert_model_instances")
    def test_optional_fields_default_none(
        self, mock_upsert: MagicMock, tmp_path: Path
    ) -> None:
        """
        Test that optional fields default to None when missing.
        """

        path = tmp_path / "variable_metadata_2024-01-01T00:00:00Z.json"
        path.write_text(
            json.dumps(
                [
                    {
                        "variable_code": "sptinc",
                        "concept": "ptinc",
                        "series_type": "s",
                        "short_name": "Income",
                        "unit": "share",
                    }
                ]
            )
        )

        processor = WIDProcessor.__new__(WIDProcessor)
        session = MagicMock()
        processor._process_variable_meta(path, session)

        v = mock_upsert.call_args[1]["model_instances"][0]
        assert v.description is None
        assert v.technical_description is None
        assert v.population_type is None
        assert v.age_group is None


# -----------------------------------------------------------------------
# TestProcessCountries
# -----------------------------------------------------------------------


class TestProcessCountries:
    """
    Tests for WIDProcessor._process_countries().
    """

    @patch("pipelines.example.process.upsert_model_instances")
    def test_upserts_country_records(
        self, mock_upsert: MagicMock, tmp_path: Path
    ) -> None:
        """
        Test that country records are upserted.
        """

        path = tmp_path / "countries_2024-01-01T00:00:00Z.json"
        path.write_text(
            json.dumps(
                [
                    {"country_code": "US", "name": "United States"},
                    {"country_code": "FR", "name": "France"},
                ]
            )
        )

        processor = WIDProcessor.__new__(WIDProcessor)
        session = MagicMock()
        processor._process_countries(path, session)

        mock_upsert.assert_called_once()
        kwargs = mock_upsert.call_args[1]
        assert kwargs["conflict_columns"] == ["country_code"]
        assert kwargs["on_conflict_update"] is True
        assert kwargs["update_columns"] == ["name"]

    @patch("pipelines.example.process.upsert_model_instances")
    def test_country_instance_types(
        self, mock_upsert: MagicMock, tmp_path: Path
    ) -> None:
        """
        Test that instances are Country ORM objects.
        """

        path = tmp_path / "countries_2024-01-01T00:00:00Z.json"
        path.write_text(json.dumps([{"country_code": "DE", "name": "Germany"}]))

        processor = WIDProcessor.__new__(WIDProcessor)
        session = MagicMock()
        processor._process_countries(path, session)

        instances = mock_upsert.call_args[1]["model_instances"]
        assert all(isinstance(i, Country) for i in instances)
        assert instances[0].country_code == "DE"
        assert instances[0].name == "Germany"

    @patch("pipelines.example.process.upsert_model_instances")
    def test_country_with_null_name(
        self, mock_upsert: MagicMock, tmp_path: Path
    ) -> None:
        """
        Test that countries with null names are handled.
        """

        path = tmp_path / "countries_2024-01-01T00:00:00Z.json"
        path.write_text(json.dumps([{"country_code": "XX"}]))

        processor = WIDProcessor.__new__(WIDProcessor)
        session = MagicMock()
        processor._process_countries(path, session)

        instances = mock_upsert.call_args[1]["model_instances"]
        assert instances[0].name is None


# -----------------------------------------------------------------------
# TestProcessObservations
# -----------------------------------------------------------------------


class TestProcessObservations:
    """
    Tests for WIDProcessor._process_observations().
    """

    @patch("pipelines.example.process.upsert_model_instances")
    def test_upserts_observations(self, mock_upsert: MagicMock, tmp_path: Path) -> None:
        """
        Test that observation records are upserted.
        """

        path = tmp_path / "observations_000_2024-01-01T00:00:00Z.json"
        path.write_text(
            json.dumps(
                {
                    "observations": [
                        {
                            "country_code": "US",
                            "variable_code": "sptinc",
                            "percentile_code": "p0p10",
                            "year": 2020,
                            "value": 0.05,
                        }
                    ],
                    "data_quality": [],
                }
            )
        )

        processor = WIDProcessor.__new__(WIDProcessor)
        session = MagicMock()
        processor._process_observations(path, session)

        # Should call upsert once (obs only, no dq records).
        mock_upsert.assert_called_once()
        kwargs = mock_upsert.call_args[1]
        assert kwargs["conflict_columns"] == [
            "country_code",
            "variable_code",
            "percentile_code",
            "year",
        ]
        assert kwargs["update_columns"] == ["value"]

    @patch("pipelines.example.process.upsert_model_instances")
    def test_upserts_data_quality(self, mock_upsert: MagicMock, tmp_path: Path) -> None:
        """
        Test that data quality records are upserted.
        """

        path = tmp_path / "observations_000_2024-01-01T00:00:00Z.json"
        path.write_text(
            json.dumps(
                {
                    "observations": [],
                    "data_quality": [
                        {
                            "country_code": "US",
                            "variable_code": "sptinc",
                            "quality_score": 4,
                            "imputation": "survey",
                            "extrapolation_ranges": "1980-2020",
                        }
                    ],
                }
            )
        )

        processor = WIDProcessor.__new__(WIDProcessor)
        session = MagicMock()
        processor._process_observations(path, session)

        mock_upsert.assert_called_once()
        kwargs = mock_upsert.call_args[1]
        assert kwargs["conflict_columns"] == [
            "country_code",
            "variable_code",
        ]
        assert "quality_score" in kwargs["update_columns"]

    @patch("pipelines.example.process.upsert_model_instances")
    def test_both_observations_and_quality(
        self, mock_upsert: MagicMock, tmp_path: Path
    ) -> None:
        """
        Test that both observations and quality are upserted.
        """

        path = tmp_path / "observations_000_2024-01-01T00:00:00Z.json"
        path.write_text(
            json.dumps(
                {
                    "observations": [
                        {
                            "country_code": "FR",
                            "variable_code": "shweal",
                            "percentile_code": "p90p100",
                            "year": 2021,
                            "value": 0.55,
                        }
                    ],
                    "data_quality": [
                        {
                            "country_code": "FR",
                            "variable_code": "shweal",
                            "quality_score": 3,
                            "imputation": None,
                            "extrapolation_ranges": None,
                        }
                    ],
                }
            )
        )

        processor = WIDProcessor.__new__(WIDProcessor)
        session = MagicMock()
        processor._process_observations(path, session)

        assert mock_upsert.call_count == 2

    @patch("pipelines.example.process.upsert_model_instances")
    def test_observation_instance_types(
        self, mock_upsert: MagicMock, tmp_path: Path
    ) -> None:
        """
        Test that instances are Observation ORM objects.
        """

        path = tmp_path / "observations_000_2024-01-01T00:00:00Z.json"
        path.write_text(
            json.dumps(
                {
                    "observations": [
                        {
                            "country_code": "US",
                            "variable_code": "sptinc",
                            "percentile_code": "p0p10",
                            "year": 2020,
                            "value": 0.05,
                        }
                    ],
                    "data_quality": [],
                }
            )
        )

        processor = WIDProcessor.__new__(WIDProcessor)
        session = MagicMock()
        processor._process_observations(path, session)

        instances = mock_upsert.call_args[1]["model_instances"]
        assert all(isinstance(i, Observation) for i in instances)

    @patch("pipelines.example.process.upsert_model_instances")
    def test_data_quality_instance_types(
        self, mock_upsert: MagicMock, tmp_path: Path
    ) -> None:
        """
        Test that instances are DataQuality ORM objects.
        """

        path = tmp_path / "observations_000_2024-01-01T00:00:00Z.json"
        path.write_text(
            json.dumps(
                {
                    "observations": [],
                    "data_quality": [
                        {
                            "country_code": "US",
                            "variable_code": "sptinc",
                            "quality_score": 4,
                            "imputation": None,
                            "extrapolation_ranges": None,
                        }
                    ],
                }
            )
        )

        processor = WIDProcessor.__new__(WIDProcessor)
        session = MagicMock()
        processor._process_observations(path, session)

        instances = mock_upsert.call_args[1]["model_instances"]
        assert all(isinstance(i, DataQuality) for i in instances)

    @patch("pipelines.example.process.upsert_model_instances")
    def test_skips_empty_observations(
        self, mock_upsert: MagicMock, tmp_path: Path
    ) -> None:
        """
        Test that empty observation list skips upsert.
        """

        path = tmp_path / "observations_000_2024-01-01T00:00:00Z.json"
        path.write_text(json.dumps({"observations": [], "data_quality": []}))

        processor = WIDProcessor.__new__(WIDProcessor)
        session = MagicMock()
        processor._process_observations(path, session)

        mock_upsert.assert_not_called()


# -----------------------------------------------------------------------
# TestProcessFileSet
# -----------------------------------------------------------------------


class TestProcessFileSet:
    """
    Tests for WIDProcessor.process_file_set() orchestration.
    """

    @patch("pipelines.example.process.upsert_model_instances")
    def test_processes_all_file_types(
        self, mock_upsert: MagicMock, tmp_path: Path
    ) -> None:
        """
        Test that all file types in a FileSet are processed.
        """

        ts = "2024-01-01T00:00:00Z"

        # Create test files.
        countries_path = tmp_path / f"countries_{ts}.json"
        countries_path.write_text(
            json.dumps([{"country_code": "US", "name": "United States"}])
        )

        var_meta_path = tmp_path / f"variable_metadata_{ts}.json"
        var_meta_path.write_text(
            json.dumps(
                [
                    {
                        "variable_code": "sptinc",
                        "concept": "ptinc",
                        "series_type": "s",
                        "short_name": "Income",
                        "unit": "share",
                    }
                ]
            )
        )

        obs_path = tmp_path / f"observations_000_{ts}.json"
        obs_path.write_text(
            json.dumps(
                {
                    "observations": [
                        {
                            "country_code": "US",
                            "variable_code": "sptinc",
                            "percentile_code": "p0p10",
                            "year": 2020,
                            "value": 0.05,
                        }
                    ],
                    "data_quality": [
                        {
                            "country_code": "US",
                            "variable_code": "sptinc",
                            "quality_score": 4,
                            "imputation": None,
                            "extrapolation_ranges": None,
                        }
                    ],
                }
            )
        )

        # Build a FileSet with all three file types.
        file_set = FileSet(
            files={
                WIDFileTypes.COUNTRY: [countries_path],
                WIDFileTypes.VARIABLE_META: [var_meta_path],
                WIDFileTypes.OBSERVATION: [obs_path],
            }
        )

        processor = WIDProcessor.__new__(WIDProcessor)
        session = MagicMock()
        processor.process_file_set(file_set, session)

        # Expect: percentiles + variable_meta + countries +
        # observations + data_quality = 5 upsert calls.
        assert mock_upsert.call_count == 5

    @patch("pipelines.example.process.upsert_model_instances")
    def test_seeds_percentiles_first(
        self, mock_upsert: MagicMock, tmp_path: Path
    ) -> None:
        """
        Test that percentile seeding happens before other processing.
        """

        ts = "2024-01-01T00:00:00Z"
        countries_path = tmp_path / f"countries_{ts}.json"
        countries_path.write_text(json.dumps([{"country_code": "US", "name": "USA"}]))

        file_set = FileSet(files={WIDFileTypes.COUNTRY: [countries_path]})

        processor = WIDProcessor.__new__(WIDProcessor)
        session = MagicMock()
        processor.process_file_set(file_set, session)

        # First call should be percentile seeding (DO NOTHING).
        first_call_kwargs = mock_upsert.call_args_list[0][1]
        assert first_call_kwargs["on_conflict_update"] is False
        assert first_call_kwargs["conflict_columns"] == ["percentile_code"]

    @patch("pipelines.example.process.upsert_model_instances")
    def test_empty_file_set(self, mock_upsert: MagicMock, tmp_path: Path) -> None:
        """
        Test processing an empty FileSet (only seeds percentiles).
        """

        file_set = FileSet()

        processor = WIDProcessor.__new__(WIDProcessor)
        session = MagicMock()
        processor.process_file_set(file_set, session)

        # Only percentile seeding should occur.
        assert mock_upsert.call_count == 1


# -----------------------------------------------------------------------
# TestConstants
# -----------------------------------------------------------------------


class TestConstants:
    """
    Tests for pipeline constants integrity.
    """

    def test_percentiles_count(self) -> None:
        """
        Test that PERCENTILES has exactly 40 entries.
        """

        assert len(PERCENTILES) == 40

    def test_percentile_tuple_structure(self) -> None:
        """
        Test that each percentile tuple has 5 elements.
        """

        for entry in PERCENTILES:
            assert len(entry) == 5

    def test_percentile_bounds_ordered(self) -> None:
        """
        Test that lower_bound < upper_bound for all percentiles.
        """

        for code, lower, upper, width, granularity in PERCENTILES:
            assert lower < upper

    def test_file_type_patterns_match(self) -> None:
        """
        Test that WIDFileTypes patterns match expected filenames.
        """

        ts = "2024-01-01T00:00:00Z"
        assert WIDFileTypes.COUNTRY.value.search(f"countries_{ts}.json")
        assert WIDFileTypes.VARIABLE_META.value.search(f"variable_metadata_{ts}.json")
        assert WIDFileTypes.OBSERVATION.value.search(f"observations_000_{ts}.json")

    def test_file_type_patterns_reject_wrong_files(self) -> None:
        """
        Test that patterns do not match unrelated filenames.
        """

        assert not WIDFileTypes.COUNTRY.value.search("variable_meta.json")
        assert not WIDFileTypes.OBSERVATION.value.search("countries.json")
        assert not WIDFileTypes.VARIABLE_META.value.search("observations.json")
