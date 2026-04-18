"""
WID pipeline extraction module.

Fetches income and wealth distribution data from the World Inequality Database
(WID.world) API and saves JSON files to the ingest directory. Supports incremental
extraction by querying the database for the most recent year already stored.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from lib.logging_utils import LOGGER

from .constants import (
    COUNTRY_BATCH_SIZE,
    PERCENTILES,
    VARIABLES,
    WID_AGE_CODE,
    WID_API_BASE_URL,
    WID_API_KEY,
    WID_POP_CODE,
)

# -------------------------------------------------------------------
# Public API
# -------------------------------------------------------------------


def extract(
    ingest_dir: Path,
    db_schema: str = "wid",
) -> List[Path]:
    """
    Extract WID data and save to the ingest directory.

    Queries the database for the most recent year already stored. On first run (empty
    database), extracts all available years. On subsequent runs, extracts from the max
    stored year onward (inclusive, since WID may revise recent estimates).

    :param ingest_dir: Path to the pipeline ingest directory.
    :param db_schema: Database schema name for WID tables.
    :return: List of paths to saved JSON files.
    """

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    saved: List[Path] = []

    # Determine incremental start year.
    max_year = _get_max_year(db_schema)
    if max_year is not None:
        LOGGER.info(
            "Existing data up to year %d. " "Extracting from %d onward.",
            max_year,
            max_year,
        )
    else:
        LOGGER.info(
            "No existing data found. " "Extracting all available years.",
        )

    # Fetch the list of all WID entity codes.
    country_codes = _fetch_country_codes()
    LOGGER.info("Found %d WID entities.", len(country_codes))

    # Fetch variable metadata and country names.
    var_meta = _fetch_variable_metadata()
    country_names = _fetch_country_names(country_codes)

    # Save countries file.
    countries = [
        {"country_code": cc, "name": country_names.get(cc)} for cc in country_codes
    ]
    saved.append(_save_json(ingest_dir, f"countries_{ts}.json", countries))

    # Save variable metadata file.
    saved.append(
        _save_json(
            ingest_dir,
            f"variable_metadata_{ts}.json",
            var_meta,
        )
    )

    # Build full WID API variable codes.
    api_var_codes = _build_api_variable_codes()
    LOGGER.info("Querying %d API variable codes.", len(api_var_codes))

    # Fetch observations in country batches.
    batches = [
        country_codes[i : i + COUNTRY_BATCH_SIZE]
        for i in range(0, len(country_codes), COUNTRY_BATCH_SIZE)
    ]

    for i, batch in enumerate(batches):
        if i > 0:
            time.sleep(1)
        try:
            obs, quality = _fetch_observation_batch(
                batch, api_var_codes, min_year=max_year
            )
        except Exception:
            LOGGER.exception(
                "Batch %d/%d (%s..%s) failed. Skipping.",
                i + 1,
                len(batches),
                batch[0],
                batch[-1],
            )
            continue

        if obs:
            path = _save_json(
                ingest_dir,
                f"observations_{i:03d}_{ts}.json",
                {"observations": obs, "data_quality": quality},
            )
            saved.append(path)
            LOGGER.info(
                "Batch %d/%d: %d observations, " "%d quality records.",
                i + 1,
                len(batches),
                len(obs),
                len(quality),
            )

    LOGGER.info("Extraction complete. %d files saved.", len(saved))
    return saved


# -------------------------------------------------------------------
# Database helpers
# -------------------------------------------------------------------


def _get_max_year(db_schema: str) -> Optional[int]:
    """
    Query the database for the maximum year in the observation table.

    Returns None if the table is empty or does not exist.

    :param db_schema: Database schema name.
    :return: Maximum year as int, or None.
    """

    try:
        from sqlalchemy import text

        from lib.sql_utils import get_engine

        engine = get_engine(schema=db_schema)
        with engine.connect() as conn:
            row = conn.execute(text("SELECT MAX(year) FROM observation")).fetchone()
            if row and row[0] is not None:
                return int(row[0])
    except Exception:
        LOGGER.info(
            "Could not query max year from database. " "Treating as first run.",
        )
    return None


# -------------------------------------------------------------------
# WID API helpers
# -------------------------------------------------------------------


def _fetch_json(url: str) -> Any:
    """
    Fetch JSON from the WID API with retry logic.

    :param url: Full API URL.
    :return: Parsed JSON response.
    """

    for attempt in range(3):
        try:
            req = Request(
                url,
                headers={
                    "User-Agent": "openetl-scaffold/1.0",
                    "x-api-key": WID_API_KEY,
                },
            )
            with urlopen(req, timeout=120) as resp:
                return json.loads(resp.read().decode())
        except (HTTPError, URLError, TimeoutError):
            if attempt < 2:
                time.sleep(3 * (attempt + 1))
                continue
            raise
    return None


def _fetch_large(url: str) -> Any:
    """
    Fetch JSON, following WID payload_too_large redirects.

    When a response payload exceeds the API gateway limit, WID returns a JSON object
    with a download_url pointing to an S3-hosted file.

    :param url: Full API URL.
    :return: Parsed JSON response.
    """

    data = _fetch_json(url)
    if isinstance(data, dict) and data.get("status") == "payload_too_large":
        dl_url = data["download_url"]
        req = Request(
            dl_url,
            headers={"User-Agent": "openetl-scaffold/1.0"},
        )
        with urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode())
    return data


def _fetch_country_codes() -> List[str]:
    """
    Fetch all entity codes available in WID for pre-tax income.

    :return: Sorted list of two-letter entity codes.
    """

    url = (
        f"{WID_API_BASE_URL}"
        f"countries-available-variables"
        f"?countries=all&variables=sptinc"
    )
    data = _fetch_large(url)
    if isinstance(data, list):
        data = data[0]
    return sorted(data.get("sptinc", {}).keys())


def _fetch_variable_metadata() -> List[Dict]:
    """
    Fetch variable descriptions from the WID metadata API.

    Queries with a single country (US) since variable metadata is the same regardless of
    country.

    :return: List of variable metadata dicts.
    """

    results = []
    for sixlet, info in VARIABLES.items():
        var_code = f"{sixlet}_p0p10" f"_{WID_AGE_CODE}_{WID_POP_CODE}"
        url = (
            f"{WID_API_BASE_URL}"
            f"countries-variables-metadata"
            f"?countries=US&variables={var_code}"
        )
        try:
            data = _fetch_large(url)
            meta = _parse_variable_meta(data, var_code)
        except Exception:
            LOGGER.warning(
                "Failed to fetch metadata for %s. " "Using fallback values.",
                sixlet,
            )
            meta = {}

        results.append(
            {
                "variable_code": sixlet,
                "concept": info["concept"],
                "series_type": info["series_type"],
                "short_name": meta.get("short_name", info["fallback_name"]),
                "description": meta.get("description"),
                "technical_description": meta.get("technical_description"),
                "unit": meta.get("unit", info["fallback_unit"]),
                "population_type": meta.get("population_type"),
                "age_group": meta.get("age_group"),
            }
        )
    return results


def _parse_variable_meta(data: Any, var_code: str) -> Dict[str, str]:
    """
    Parse variable metadata from the nested WID response.

    :param data: Raw metadata API response.
    :param var_code: Full WID variable code.
    :return: Dict with parsed metadata fields.
    """

    result: Dict[str, str] = {}
    try:
        func_list = data[0]["metadata_func"]
        for item in func_list:
            if var_code not in item:
                continue
            for section in item[var_code]:
                if "name" in section:
                    n = section["name"]
                    result["short_name"] = n.get("shortname", "").strip()
                    result["description"] = n.get("simpledes", "").strip()
                    result["technical_description"] = n.get("technicaldes", "").strip()
                elif "pop" in section:
                    result["population_type"] = (
                        section["pop"].get("shortdes", "").strip()
                    )
                elif "age" in section:
                    result["age_group"] = section["age"].get("fullname", "").strip()
                elif "units" in section:
                    for u in section["units"]:
                        result["unit"] = u.get("metadata", {}).get("unit", "share")
                        break
    except (IndexError, KeyError, TypeError):
        pass
    return result


def _fetch_country_names(
    country_codes: List[str],
) -> Dict[str, str]:
    """
    Fetch country names from the WID metadata API.

    :param country_codes: List of entity codes.
    :return: Dict mapping country code to name.
    """

    names: Dict[str, str] = {}
    var_code = f"sptinc_p0p10_{WID_AGE_CODE}_{WID_POP_CODE}"
    batch_size = 50

    for i in range(0, len(country_codes), batch_size):
        batch = country_codes[i : i + batch_size]
        url = (
            f"{WID_API_BASE_URL}"
            f"countries-variables-metadata"
            f"?countries={','.join(batch)}"
            f"&variables={var_code}"
        )
        try:
            data = _fetch_large(url)
            func_list = data[0]["metadata_func"]
            for item in func_list:
                if var_code not in item:
                    continue
                for section in item[var_code]:
                    if "units" not in section:
                        continue
                    for u in section["units"]:
                        code = u.get("country")
                        name = u.get("country_name")
                        if code and name:
                            names[code] = name
        except Exception:
            LOGGER.warning(
                "Failed to fetch country names for " "batch %d. Continuing.",
                i // batch_size + 1,
            )
    return names


# -------------------------------------------------------------------
# Variable code construction
# -------------------------------------------------------------------


def _build_api_variable_codes() -> List[str]:
    """
    Build full WID API variable codes from config.

    Combines each sixlet with each percentile code, the age code, and the population
    code.

    :return: List of full variable codes for API queries.
    """

    codes = []
    for sixlet in VARIABLES:
        for perc_code, *_ in PERCENTILES:
            codes.append(f"{sixlet}_{perc_code}" f"_{WID_AGE_CODE}_{WID_POP_CODE}")
    return codes


# -------------------------------------------------------------------
# Observation fetching
# -------------------------------------------------------------------


def _fetch_observation_batch(
    country_codes: List[str],
    api_var_codes: List[str],
    min_year: Optional[int] = None,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Fetch observation data for a batch of countries.

    Returns flattened observation records and data quality records extracted from the
    response metadata.

    :param country_codes: Country codes for this batch.
    :param api_var_codes: Full WID variable codes to query.
    :param min_year: Minimum year to include (inclusive). None means include all years.
    :return: Tuple of (observations, data_quality) record lists.
    """

    url = (
        f"{WID_API_BASE_URL}countries-variables"
        f"?countries={','.join(country_codes)}"
        f"&variables={','.join(api_var_codes)}"
        f"&years=all"
    )
    data = _fetch_large(url)

    observations: List[Dict] = []
    quality: Dict[tuple, Dict] = {}

    for full_code, entries in data.items():
        # Parse: sptinc_p0p10_992_j -> sixlet=sptinc,
        # percentile=p0p10.
        parts = full_code.split("_")
        sixlet = parts[0]
        percentile = parts[1]

        for entry in entries:
            for cc, cd in entry.items():
                meta = cd.get("meta", {})

                # Extract observations.
                for v in cd.get("values", []):
                    year = v["y"]
                    if min_year is not None and year < min_year:
                        continue
                    observations.append(
                        {
                            "country_code": cc,
                            "variable_code": sixlet,
                            "percentile_code": percentile,
                            "year": year,
                            "value": v.get("v"),
                        }
                    )

                # Extract data quality (once per
                # country + variable).
                qk = (cc, sixlet)
                if qk not in quality:
                    quality[qk] = {
                        "country_code": cc,
                        "variable_code": sixlet,
                        "quality_score": _safe_int(meta.get("data_quality")),
                        "imputation": meta.get("imputation"),
                        "extrapolation_ranges": meta.get("extrapolation"),
                    }

    return observations, list(quality.values())


# -------------------------------------------------------------------
# File I/O
# -------------------------------------------------------------------


def _save_json(directory: Path, filename: str, data: Any) -> Path:
    """
    Save data as a JSON file in the given directory.

    :param directory: Target directory.
    :param filename: File name.
    :param data: JSON-serializable data.
    :return: Path to the saved file.
    """

    path = directory / filename
    with open(path, "w") as f:
        json.dump(data, f)
    LOGGER.info("Saved %s (%d bytes).", path.name, path.stat().st_size)
    return path


def _safe_int(val: Any) -> Optional[int]:
    """
    Convert a value to int, returning None on failure.

    :param val: Value to convert.
    :return: Integer or None.
    """

    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None
