"""
WID pipeline processor.

Implements the domain logic for parsing extracted WID JSON files, seeding dimension
tables, and upserting observations and data quality records into the database.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy.orm import Session

from lib.filesystem_utils import FileSet
from lib.logging_utils import LOGGER
from lib.sql_utils import upsert_model_instances
from lib.task_utils import Processor

from .constants import PERCENTILES, WIDFileTypes
from .sqla_models import (
    Country,
    DataQuality,
    Observation,
    Percentile,
    Variable,
)


class WIDProcessor(Processor):
    """
    Processor for WID pipeline data files.

    Handles three file types produced by the extract task:
      - COUNTRY files: upsert country dimension records.
      - VARIABLE_META files: upsert variable dimension records
        and seed the percentile dimension on first run.
      - OBSERVATION files: upsert observation fact records and
        data quality metadata.
    """

    def process_file_set(self, file_set: FileSet, session: Session) -> None:
        """
        Parse files and upsert rows into the database.

        :param file_set: FileSet to process.
        :param session: SQLAlchemy Session object.
        """

        # Seed percentile dimension (idempotent).
        self._seed_percentiles(session)

        # Process variable metadata files first (dimension
        # must exist before observations reference it).
        for path in file_set.get_files(WIDFileTypes.VARIABLE_META):
            self._process_variable_meta(path, session)

        # Process country files.
        for path in file_set.get_files(WIDFileTypes.COUNTRY):
            self._process_countries(path, session)

        # Process observation files.
        for path in file_set.get_files(WIDFileTypes.OBSERVATION):
            self._process_observations(path, session)

    def _seed_percentiles(self, session: Session) -> None:
        """
        Seed the percentile dimension table from constants.

        Uses ON CONFLICT DO NOTHING so this is safe to call on every run.

        :param session: SQLAlchemy Session object.
        """

        instances = [
            Percentile(
                percentile_code=code,
                lower_bound=lower,
                upper_bound=upper,
                width=width,
                granularity=granularity,
            )
            for code, lower, upper, width, granularity in PERCENTILES
        ]
        upsert_model_instances(
            session=session,
            model_instances=instances,
            conflict_columns=["percentile_code"],
            on_conflict_update=False,
        )
        LOGGER.info(
            "Percentile dimension seeded (%d rows).",
            len(instances),
        )

    def _process_variable_meta(self, path: Path, session: Session) -> None:
        """
        Parse variable metadata JSON and upsert to the variable table.

        :param path: Path to the variable metadata JSON file.
        :param session: SQLAlchemy Session object.
        """

        records: List[Dict[str, Any]] = _read_json(path)
        instances = [
            Variable(
                variable_code=r["variable_code"],
                concept=r["concept"],
                series_type=r["series_type"],
                short_name=r["short_name"],
                description=r.get("description"),
                technical_description=r.get("technical_description"),
                unit=r["unit"],
                population_type=r.get("population_type"),
                age_group=r.get("age_group"),
            )
            for r in records
        ]
        upsert_model_instances(
            session=session,
            model_instances=instances,
            conflict_columns=["variable_code"],
            on_conflict_update=True,
            update_columns=[
                "short_name",
                "description",
                "technical_description",
                "unit",
                "population_type",
                "age_group",
            ],
        )
        LOGGER.info(
            "Upserted %d variable records from %s.",
            len(instances),
            path.name,
        )

    def _process_countries(self, path: Path, session: Session) -> None:
        """
        Parse country JSON and upsert to the country table.

        :param path: Path to the countries JSON file.
        :param session: SQLAlchemy Session object.
        """

        records: List[Dict[str, Any]] = _read_json(path)
        instances = [
            Country(
                country_code=r["country_code"],
                name=r.get("name"),
            )
            for r in records
        ]
        upsert_model_instances(
            session=session,
            model_instances=instances,
            conflict_columns=["country_code"],
            on_conflict_update=True,
            update_columns=["name"],
        )
        LOGGER.info(
            "Upserted %d country records from %s.",
            len(instances),
            path.name,
        )

    def _process_observations(self, path: Path, session: Session) -> None:
        """
        Parse observation JSON and upsert observations and data quality.

        The observation file contains two keys: "observations"
        (list of fact records) and "data_quality" (list of
        quality metadata records).

        :param path: Path to the observations JSON file.
        :param session: SQLAlchemy Session object.
        """

        data: Dict[str, Any] = _read_json(path)

        # Upsert observations.
        obs_records = data.get("observations", [])
        if obs_records:
            obs_instances = [
                Observation(
                    country_code=r["country_code"],
                    variable_code=r["variable_code"],
                    percentile_code=r["percentile_code"],
                    year=r["year"],
                    value=r.get("value"),
                )
                for r in obs_records
            ]
            upsert_model_instances(
                session=session,
                model_instances=obs_instances,
                conflict_columns=[
                    "country_code",
                    "variable_code",
                    "percentile_code",
                    "year",
                ],
                on_conflict_update=True,
                update_columns=["value"],
            )
            LOGGER.info(
                "Upserted %d observations from %s.",
                len(obs_instances),
                path.name,
            )

        # Upsert data quality.
        dq_records = data.get("data_quality", [])
        if dq_records:
            dq_instances = [
                DataQuality(
                    country_code=r["country_code"],
                    variable_code=r["variable_code"],
                    quality_score=r.get("quality_score"),
                    imputation=r.get("imputation"),
                    extrapolation_ranges=r.get("extrapolation_ranges"),
                )
                for r in dq_records
            ]
            upsert_model_instances(
                session=session,
                model_instances=dq_instances,
                conflict_columns=[
                    "country_code",
                    "variable_code",
                ],
                on_conflict_update=True,
                update_columns=[
                    "quality_score",
                    "imputation",
                    "extrapolation_ranges",
                ],
            )
            LOGGER.info(
                "Upserted %d data quality records from %s.",
                len(dq_instances),
                path.name,
            )


def _read_json(path: Path) -> Any:
    """
    Read and parse a JSON file.

    :param path: Path to the JSON file.
    :return: Parsed JSON data.
    """

    with open(path) as f:
        return json.load(f)
