"""
Unit tests for pipelines.example.extract module.

This test suite covers:
    - _safe_int() type conversion helper.
    - _save_json() file I/O.
    - _build_api_variable_codes() variable code construction.
    - _fetch_json() retry logic.
    - _fetch_large() payload_too_large redirect handling.
    - _fetch_country_codes() entity list parsing.
    - _fetch_variable_metadata() metadata assembly with fallbacks.
    - _fetch_country_names() name resolution from metadata API.
    - _parse_variable_meta() nested response parsing.
    - _fetch_observation_batch() flattening and quality extraction.
    - _get_max_year() database query.
    - extract() end-to-end orchestration.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipelines.example.extract import (
    _build_api_variable_codes,
    _fetch_country_codes,
    _fetch_country_names,
    _fetch_json,
    _fetch_large,
    _fetch_observation_batch,
    _fetch_variable_metadata,
    _get_max_year,
    _parse_variable_meta,
    _safe_int,
    _save_json,
    extract,
)

# -----------------------------------------------------------------------
# TestSafeInt
# -----------------------------------------------------------------------


class TestSafeInt:
    """
    Tests for _safe_int() helper.
    """

    def test_none_returns_none(self) -> None:
        """
        Test that None input returns None.
        """

        assert _safe_int(None) is None

    def test_int_passthrough(self) -> None:
        """
        Test that int values pass through unchanged.
        """

        assert _safe_int(5) == 5

    def test_string_numeric(self) -> None:
        """
        Test that numeric strings are converted to int.
        """

        assert _safe_int("42") == 42

    def test_string_non_numeric(self) -> None:
        """
        Test that non-numeric strings return None.
        """

        assert _safe_int("abc") is None

    def test_float_truncates(self) -> None:
        """
        Test that float values are truncated to int.
        """

        assert _safe_int(3.9) == 3


# -----------------------------------------------------------------------
# TestSaveJson
# -----------------------------------------------------------------------


class TestSaveJson:
    """
    Tests for _save_json() file I/O.
    """

    def test_saves_file(self, tmp_path: Path) -> None:
        """
        Test that JSON data is written to disk.
        """

        data = [{"key": "value"}]
        result = _save_json(tmp_path, "test.json", data)
        assert result == tmp_path / "test.json"
        assert result.exists()

    def test_content_matches(self, tmp_path: Path) -> None:
        """
        Test that saved content matches input data.
        """

        data = {"a": 1, "b": [2, 3]}
        _save_json(tmp_path, "out.json", data)
        with open(tmp_path / "out.json") as f:
            loaded = json.load(f)
        assert loaded == data

    def test_returns_path(self, tmp_path: Path) -> None:
        """
        Test that the returned path is correct.
        """

        result = _save_json(tmp_path, "file.json", [])
        assert isinstance(result, Path)
        assert result.name == "file.json"


# -----------------------------------------------------------------------
# TestBuildApiVariableCodes
# -----------------------------------------------------------------------


class TestBuildApiVariableCodes:
    """
    Tests for _build_api_variable_codes().
    """

    def test_returns_list(self) -> None:
        """
        Test that result is a list of strings.
        """

        codes = _build_api_variable_codes()
        assert isinstance(codes, list)
        assert all(isinstance(c, str) for c in codes)

    def test_count_matches_variables_times_percentiles(self) -> None:
        """
        Test that code count equals variables * percentiles.
        """

        from pipelines.example.constants import PERCENTILES, VARIABLES

        codes = _build_api_variable_codes()
        assert len(codes) == len(VARIABLES) * len(PERCENTILES)

    def test_code_format(self) -> None:
        """
        Test that each code has the expected format.
        """

        codes = _build_api_variable_codes()
        # Format: sixlet_percentile_ageCode_popCode.
        for code in codes:
            parts = code.split("_")
            assert len(parts) == 4
            assert parts[2] == "992"
            assert parts[3] == "j"

    def test_contains_both_sixlets(self) -> None:
        """
        Test that both sptinc and shweal sixlets are present.
        """

        codes = _build_api_variable_codes()
        sixlets = {c.split("_")[0] for c in codes}
        assert "sptinc" in sixlets
        assert "shweal" in sixlets


# -----------------------------------------------------------------------
# TestFetchJson
# -----------------------------------------------------------------------


class TestFetchJson:
    """
    Tests for _fetch_json() HTTP fetching with retries.
    """

    @patch("pipelines.example.extract.urlopen")
    def test_success(self, mock_urlopen: MagicMock) -> None:
        """
        Test successful JSON fetch.
        """

        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"ok": true}'
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = _fetch_json("http://example.com/api")
        assert result == {"ok": True}

    @patch("pipelines.example.extract.time.sleep")
    @patch("pipelines.example.extract.urlopen")
    def test_retries_on_error(
        self, mock_urlopen: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """
        Test that transient errors trigger retries.
        """

        from urllib.error import URLError

        mock_resp = MagicMock()
        mock_resp.read.return_value = b"[]"
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        mock_urlopen.side_effect = [
            URLError("timeout"),
            mock_resp,
        ]

        result = _fetch_json("http://example.com/api")
        assert result == []
        mock_sleep.assert_called_once_with(3)

    @patch("pipelines.example.extract.time.sleep")
    @patch("pipelines.example.extract.urlopen")
    def test_raises_after_max_retries(
        self, mock_urlopen: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """
        Test that exception is raised after all retries exhausted.
        """

        from urllib.error import URLError

        mock_urlopen.side_effect = URLError("persistent failure")

        with pytest.raises(URLError):
            _fetch_json("http://example.com/api")

        assert mock_sleep.call_count == 2


# -----------------------------------------------------------------------
# TestFetchLarge
# -----------------------------------------------------------------------


class TestFetchLarge:
    """
    Tests for _fetch_large() payload redirect handling.
    """

    @patch("pipelines.example.extract._fetch_json")
    def test_normal_response_passthrough(self, mock_fetch: MagicMock) -> None:
        """
        Test that normal responses pass through unchanged.
        """

        mock_fetch.return_value = [{"data": "value"}]
        result = _fetch_large("http://api.example.com")
        assert result == [{"data": "value"}]

    @patch("pipelines.example.extract.urlopen")
    @patch("pipelines.example.extract._fetch_json")
    def test_payload_too_large_redirect(
        self, mock_fetch: MagicMock, mock_urlopen: MagicMock
    ) -> None:
        """
        Test that payload_too_large triggers download from S3 URL.
        """

        mock_fetch.return_value = {
            "status": "payload_too_large",
            "download_url": "http://s3.example.com/data.json",
        }

        mock_resp = MagicMock()
        mock_resp.read.return_value = b'[{"redirected": true}]'
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = _fetch_large("http://api.example.com")
        assert result == [{"redirected": True}]


# -----------------------------------------------------------------------
# TestFetchCountryCodes
# -----------------------------------------------------------------------


class TestFetchCountryCodes:
    """
    Tests for _fetch_country_codes().
    """

    @patch("pipelines.example.extract._fetch_large")
    def test_returns_sorted_codes(self, mock_fetch: MagicMock) -> None:
        """
        Test that country codes are returned sorted.
        """

        mock_fetch.return_value = [{"sptinc": {"FR": {}, "US": {}, "DE": {}}}]
        result = _fetch_country_codes()
        assert result == ["DE", "FR", "US"]

    @patch("pipelines.example.extract._fetch_large")
    def test_unwraps_list_response(self, mock_fetch: MagicMock) -> None:
        """
        Test that list-wrapped responses are unwrapped.
        """

        mock_fetch.return_value = [{"sptinc": {"BR": {}, "AR": {}}}]
        result = _fetch_country_codes()
        assert result == ["AR", "BR"]

    @patch("pipelines.example.extract._fetch_large")
    def test_dict_response(self, mock_fetch: MagicMock) -> None:
        """
        Test that dict responses (non-list) are handled.
        """

        mock_fetch.return_value = {"sptinc": {"JP": {}, "CN": {}}}
        result = _fetch_country_codes()
        assert result == ["CN", "JP"]


# -----------------------------------------------------------------------
# TestParseVariableMeta
# -----------------------------------------------------------------------


class TestParseVariableMeta:
    """
    Tests for _parse_variable_meta() nested response parsing.
    """

    def test_extracts_name_fields(self) -> None:
        """
        Test extraction of name-related metadata.
        """

        data = [
            {
                "metadata_func": [
                    {
                        "sptinc_p0p10_992_j": [
                            {
                                "name": {
                                    "shortname": "Income share",
                                    "simpledes": "Simple description.",
                                    "technicaldes": "Technical desc.",
                                }
                            }
                        ]
                    }
                ]
            }
        ]
        result = _parse_variable_meta(data, "sptinc_p0p10_992_j")
        assert result["short_name"] == "Income share"
        assert result["description"] == "Simple description."
        assert result["technical_description"] == "Technical desc."

    def test_extracts_population_type(self) -> None:
        """
        Test extraction of population type metadata.
        """

        data = [
            {
                "metadata_func": [
                    {
                        "sptinc_p0p10_992_j": [
                            {"pop": {"shortdes": "Equal-split adults"}}
                        ]
                    }
                ]
            }
        ]
        result = _parse_variable_meta(data, "sptinc_p0p10_992_j")
        assert result["population_type"] == "Equal-split adults"

    def test_extracts_age_group(self) -> None:
        """
        Test extraction of age group metadata.
        """

        data = [
            {
                "metadata_func": [
                    {"sptinc_p0p10_992_j": [{"age": {"fullname": "Adults 20+"}}]}
                ]
            }
        ]
        result = _parse_variable_meta(data, "sptinc_p0p10_992_j")
        assert result["age_group"] == "Adults 20+"

    def test_extracts_unit(self) -> None:
        """
        Test extraction of unit from units section.
        """

        data = [
            {
                "metadata_func": [
                    {
                        "sptinc_p0p10_992_j": [
                            {
                                "units": [
                                    {
                                        "metadata": {"unit": "share"},
                                        "country": "US",
                                        "country_name": "USA",
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
        result = _parse_variable_meta(data, "sptinc_p0p10_992_j")
        assert result["unit"] == "share"

    def test_empty_on_malformed_data(self) -> None:
        """
        Test that malformed data returns empty dict.
        """

        result = _parse_variable_meta([], "sptinc_p0p10_992_j")
        assert result == {}

    def test_empty_on_missing_var_code(self) -> None:
        """
        Test that missing var_code in response returns empty dict.
        """

        data = [{"metadata_func": [{"other_code": []}]}]
        result = _parse_variable_meta(data, "sptinc_p0p10_992_j")
        assert result == {}


# -----------------------------------------------------------------------
# TestFetchVariableMetadata
# -----------------------------------------------------------------------


class TestFetchVariableMetadata:
    """
    Tests for _fetch_variable_metadata().
    """

    @patch("pipelines.example.extract._fetch_large")
    def test_returns_metadata_for_each_variable(self, mock_fetch: MagicMock) -> None:
        """
        Test that metadata is returned for each configured variable.
        """

        from pipelines.example.constants import VARIABLES

        mock_fetch.return_value = [
            {
                "metadata_func": [
                    {
                        "sptinc_p0p10_992_j": [
                            {
                                "name": {
                                    "shortname": "Income",
                                    "simpledes": "Desc.",
                                    "technicaldes": "Tech.",
                                }
                            }
                        ]
                    }
                ]
            }
        ]

        result = _fetch_variable_metadata()
        assert len(result) == len(VARIABLES)

    @patch("pipelines.example.extract._fetch_large")
    def test_uses_fallback_on_api_failure(self, mock_fetch: MagicMock) -> None:
        """
        Test that fallback values are used when API fails.
        """

        mock_fetch.side_effect = Exception("API down")
        result = _fetch_variable_metadata()

        # Should still return results using fallback values.
        assert len(result) == 2
        assert result[0]["short_name"] == "Pre-tax national income share"
        assert result[1]["short_name"] == "Net personal wealth share"

    @patch("pipelines.example.extract._fetch_large")
    def test_metadata_structure(self, mock_fetch: MagicMock) -> None:
        """
        Test that returned metadata has expected keys.
        """

        mock_fetch.side_effect = Exception("API down")
        result = _fetch_variable_metadata()

        expected_keys = {
            "variable_code",
            "concept",
            "series_type",
            "short_name",
            "description",
            "technical_description",
            "unit",
            "population_type",
            "age_group",
        }
        for entry in result:
            assert set(entry.keys()) == expected_keys


# -----------------------------------------------------------------------
# TestFetchCountryNames
# -----------------------------------------------------------------------


class TestFetchCountryNames:
    """
    Tests for _fetch_country_names().
    """

    @patch("pipelines.example.extract._fetch_large")
    def test_extracts_names(self, mock_fetch: MagicMock) -> None:
        """
        Test that country names are extracted from metadata.
        """

        mock_fetch.return_value = [
            {
                "metadata_func": [
                    {
                        "sptinc_p0p10_992_j": [
                            {
                                "units": [
                                    {
                                        "country": "US",
                                        "country_name": "United States",
                                    },
                                    {
                                        "country": "FR",
                                        "country_name": "France",
                                    },
                                ]
                            }
                        ]
                    }
                ]
            }
        ]

        result = _fetch_country_names(["US", "FR"])
        assert result == {"US": "United States", "FR": "France"}

    @patch("pipelines.example.extract._fetch_large")
    def test_handles_api_failure(self, mock_fetch: MagicMock) -> None:
        """
        Test that API failure returns partial results.
        """

        mock_fetch.side_effect = Exception("Network error")
        result = _fetch_country_names(["US", "FR"])
        assert result == {}

    @patch("pipelines.example.extract._fetch_large")
    def test_batches_requests(self, mock_fetch: MagicMock) -> None:
        """
        Test that large lists are batched (50 per request).
        """

        mock_fetch.return_value = [{"metadata_func": [{"sptinc_p0p10_992_j": []}]}]
        codes = [f"C{i:02d}" for i in range(75)]
        _fetch_country_names(codes)
        assert mock_fetch.call_count == 2


# -----------------------------------------------------------------------
# TestFetchObservationBatch
# -----------------------------------------------------------------------


class TestFetchObservationBatch:
    """
    Tests for _fetch_observation_batch().
    """

    @patch("pipelines.example.extract._fetch_large")
    def test_flattens_observations(self, mock_fetch: MagicMock) -> None:
        """
        Test that nested API response is flattened to records.
        """

        mock_fetch.return_value = {
            "sptinc_p0p10_992_j": [
                {
                    "US": {
                        "values": [
                            {"y": 2020, "v": 0.25},
                            {"y": 2021, "v": 0.26},
                        ],
                        "meta": {"data_quality": "3"},
                    }
                }
            ]
        }

        obs, quality = _fetch_observation_batch(["US"], ["sptinc_p0p10_992_j"])

        assert len(obs) == 2
        assert obs[0]["country_code"] == "US"
        assert obs[0]["variable_code"] == "sptinc"
        assert obs[0]["percentile_code"] == "p0p10"
        assert obs[0]["year"] == 2020
        assert obs[0]["value"] == 0.25

    @patch("pipelines.example.extract._fetch_large")
    def test_extracts_data_quality(self, mock_fetch: MagicMock) -> None:
        """
        Test that data quality metadata is extracted.
        """

        mock_fetch.return_value = {
            "sptinc_p0p10_992_j": [
                {
                    "US": {
                        "values": [{"y": 2020, "v": 0.5}],
                        "meta": {
                            "data_quality": "4",
                            "imputation": "survey",
                            "extrapolation": "1980-2020",
                        },
                    }
                }
            ]
        }

        _, quality = _fetch_observation_batch(["US"], ["sptinc_p0p10_992_j"])

        assert len(quality) == 1
        assert quality[0]["country_code"] == "US"
        assert quality[0]["variable_code"] == "sptinc"
        assert quality[0]["quality_score"] == 4
        assert quality[0]["imputation"] == "survey"
        assert quality[0]["extrapolation_ranges"] == "1980-2020"

    @patch("pipelines.example.extract._fetch_large")
    def test_min_year_filter(self, mock_fetch: MagicMock) -> None:
        """
        Test that min_year filters out older observations.
        """

        mock_fetch.return_value = {
            "sptinc_p0p10_992_j": [
                {
                    "US": {
                        "values": [
                            {"y": 2018, "v": 0.2},
                            {"y": 2020, "v": 0.25},
                        ],
                        "meta": {},
                    }
                }
            ]
        }

        obs, _ = _fetch_observation_batch(["US"], ["sptinc_p0p10_992_j"], min_year=2020)

        assert len(obs) == 1
        assert obs[0]["year"] == 2020

    @patch("pipelines.example.extract._fetch_large")
    def test_deduplicates_quality_per_country_variable(
        self, mock_fetch: MagicMock
    ) -> None:
        """
        Test that quality records are deduplicated by (country, variable).
        """

        mock_fetch.return_value = {
            "sptinc_p0p10_992_j": [
                {
                    "US": {
                        "values": [{"y": 2020, "v": 0.1}],
                        "meta": {"data_quality": "3"},
                    }
                }
            ],
            "sptinc_p10p20_992_j": [
                {
                    "US": {
                        "values": [{"y": 2020, "v": 0.2}],
                        "meta": {"data_quality": "4"},
                    }
                }
            ],
        }

        _, quality = _fetch_observation_batch(
            ["US"],
            ["sptinc_p0p10_992_j", "sptinc_p10p20_992_j"],
        )

        # Same (US, sptinc) pair: only first encountered is kept.
        assert len(quality) == 1


# -----------------------------------------------------------------------
# TestGetMaxYear
# -----------------------------------------------------------------------


class TestGetMaxYear:
    """
    Tests for _get_max_year() database query.
    """

    @patch("lib.sql_utils.get_engine")
    def test_returns_year_when_data_exists(self, mock_get_engine: MagicMock) -> None:
        """
        Test that max year is returned when data exists.
        """

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = (2023,)
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__ = lambda s: mock_conn
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_get_engine.return_value = mock_engine

        result = _get_max_year("wid")
        assert result == 2023

    @patch("lib.sql_utils.get_engine")
    def test_returns_none_when_empty(self, mock_get_engine: MagicMock) -> None:
        """
        Test that None is returned when table is empty.
        """

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = (None,)
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__ = lambda s: mock_conn
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_get_engine.return_value = mock_engine

        result = _get_max_year("wid")
        assert result is None

    @patch("lib.sql_utils.get_engine")
    def test_returns_none_on_exception(self, mock_get_engine: MagicMock) -> None:
        """
        Test that exceptions return None (first-run behavior).
        """

        mock_get_engine.side_effect = Exception("No DB")
        result = _get_max_year("wid")
        assert result is None


# -----------------------------------------------------------------------
# TestExtract
# -----------------------------------------------------------------------


class TestExtract:
    """
    Tests for the extract() orchestration function.
    """

    @patch("pipelines.example.extract._fetch_observation_batch")
    @patch("pipelines.example.extract._fetch_country_names")
    @patch("pipelines.example.extract._fetch_variable_metadata")
    @patch("pipelines.example.extract._fetch_country_codes")
    @patch("pipelines.example.extract._get_max_year")
    @patch("pipelines.example.extract.time.sleep")
    def test_saves_countries_and_metadata(
        self,
        mock_sleep: MagicMock,
        mock_max_year: MagicMock,
        mock_codes: MagicMock,
        mock_var_meta: MagicMock,
        mock_names: MagicMock,
        mock_obs_batch: MagicMock,
        tmp_path: Path,
    ) -> None:
        """
        Test that countries and variable metadata files are saved.
        """

        mock_max_year.return_value = None
        mock_codes.return_value = ["US", "FR"]
        mock_var_meta.return_value = [{"variable_code": "sptinc", "concept": "ptinc"}]
        mock_names.return_value = {
            "US": "United States",
            "FR": "France",
        }
        mock_obs_batch.return_value = ([], [])

        result = extract(ingest_dir=tmp_path, db_schema="wid")

        # Should save countries + variable_metadata (no obs since
        # batch returned empty).
        assert len(result) == 2
        assert any("countries_" in p.name for p in result)
        assert any("variable_metadata_" in p.name for p in result)

    @patch("pipelines.example.extract._fetch_observation_batch")
    @patch("pipelines.example.extract._fetch_country_names")
    @patch("pipelines.example.extract._fetch_variable_metadata")
    @patch("pipelines.example.extract._fetch_country_codes")
    @patch("pipelines.example.extract._get_max_year")
    @patch("pipelines.example.extract.time.sleep")
    def test_saves_observation_files(
        self,
        mock_sleep: MagicMock,
        mock_max_year: MagicMock,
        mock_codes: MagicMock,
        mock_var_meta: MagicMock,
        mock_names: MagicMock,
        mock_obs_batch: MagicMock,
        tmp_path: Path,
    ) -> None:
        """
        Test that observation batch files are saved.
        """

        mock_max_year.return_value = 2020
        mock_codes.return_value = ["US"]
        mock_var_meta.return_value = []
        mock_names.return_value = {"US": "United States"}
        mock_obs_batch.return_value = (
            [{"country_code": "US", "year": 2020, "value": 0.5}],
            [{"country_code": "US", "quality_score": 3}],
        )

        result = extract(ingest_dir=tmp_path, db_schema="wid")

        obs_files = [p for p in result if "observations_" in p.name]
        assert len(obs_files) == 1

    @patch("pipelines.example.extract._fetch_observation_batch")
    @patch("pipelines.example.extract._fetch_country_names")
    @patch("pipelines.example.extract._fetch_variable_metadata")
    @patch("pipelines.example.extract._fetch_country_codes")
    @patch("pipelines.example.extract._get_max_year")
    @patch("pipelines.example.extract.time.sleep")
    def test_continues_on_batch_failure(
        self,
        mock_sleep: MagicMock,
        mock_max_year: MagicMock,
        mock_codes: MagicMock,
        mock_var_meta: MagicMock,
        mock_names: MagicMock,
        mock_obs_batch: MagicMock,
        tmp_path: Path,
    ) -> None:
        """
        Test that a failed batch does not stop the extraction.
        """

        mock_max_year.return_value = None
        # 40 codes to create 2 batches of 20.
        mock_codes.return_value = [f"C{i:02d}" for i in range(40)]
        mock_var_meta.return_value = []
        mock_names.return_value = {}
        mock_obs_batch.side_effect = [
            Exception("Batch 1 failed"),
            ([{"country_code": "C20", "year": 2020, "value": 0.1}], []),
        ]

        result = extract(ingest_dir=tmp_path, db_schema="wid")

        # Should still save countries + metadata + 1 obs file.
        obs_files = [p for p in result if "observations_" in p.name]
        assert len(obs_files) == 1

    @patch("pipelines.example.extract._fetch_observation_batch")
    @patch("pipelines.example.extract._fetch_country_names")
    @patch("pipelines.example.extract._fetch_variable_metadata")
    @patch("pipelines.example.extract._fetch_country_codes")
    @patch("pipelines.example.extract._get_max_year")
    @patch("pipelines.example.extract.time.sleep")
    def test_sleeps_between_batches(
        self,
        mock_sleep: MagicMock,
        mock_max_year: MagicMock,
        mock_codes: MagicMock,
        mock_var_meta: MagicMock,
        mock_names: MagicMock,
        mock_obs_batch: MagicMock,
        tmp_path: Path,
    ) -> None:
        """
        Test that 1s delay is applied between observation batches.
        """

        mock_max_year.return_value = None
        mock_codes.return_value = [f"C{i:02d}" for i in range(40)]
        mock_var_meta.return_value = []
        mock_names.return_value = {}
        mock_obs_batch.return_value = ([], [])

        extract(ingest_dir=tmp_path, db_schema="wid")

        mock_sleep.assert_called_once_with(1)
