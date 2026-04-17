"""
SQLAlchemy ORM models for the WID pipeline.

Keep in sync with tables.ddl: same column names, types, and constraints.
"""

from sqlalchemy import (
    Column,
    Integer,
    Numeric,
    String,
    Text,
)

from lib.sql_utils import fkey, make_base

# All tables use upsert semantics. update_ts is injected
# automatically by the base class.
UpsertBase = make_base(schema="wid", include_update_ts=True)


class Country(UpsertBase):
    """
    Country and entity dimension from WID.world.
    """

    __tablename__ = "country"

    country_code = Column(String(2), primary_key=True)
    name = Column(String(255))
    # create_ts TIMESTAMPTZ is managed by the database default.
    # update_ts TIMESTAMPTZ is added automatically by UpsertBase.


class Variable(UpsertBase):
    """
    WID variable dimension (sixlet codes).
    """

    __tablename__ = "variable"

    variable_code = Column(String(10), primary_key=True)
    concept = Column(String(5), nullable=False)
    series_type = Column(String(1), nullable=False)
    short_name = Column(String(255), nullable=False)
    description = Column(Text)
    technical_description = Column(Text)
    unit = Column(String(50), nullable=False)
    population_type = Column(String(50))
    age_group = Column(String(100))


class Percentile(UpsertBase):
    """
    Distribution percentile dimension.
    """

    __tablename__ = "percentile"

    percentile_code = Column(String(20), primary_key=True)
    lower_bound = Column(Numeric(6, 2), nullable=False)
    upper_bound = Column(Numeric(6, 2), nullable=False)
    width = Column(Numeric(6, 2), nullable=False)
    granularity = Column(String(15), nullable=False)


class Observation(UpsertBase):
    """Fact table: distribution values per country/variable/percentile/year."""

    __tablename__ = "observation"

    country_code = Column(
        String(2),
        fkey("wid", "country", "country_code"),
        primary_key=True,
    )
    variable_code = Column(
        String(10),
        fkey("wid", "variable", "variable_code"),
        primary_key=True,
    )
    percentile_code = Column(
        String(20),
        fkey("wid", "percentile", "percentile_code"),
        primary_key=True,
    )
    year = Column(Integer, primary_key=True)
    value = Column(Numeric(10, 6))


class DataQuality(UpsertBase):
    """
    Data quality metadata per country and variable.
    """

    __tablename__ = "data_quality"

    country_code = Column(
        String(2),
        fkey("wid", "country", "country_code"),
        primary_key=True,
    )
    variable_code = Column(
        String(10),
        fkey("wid", "variable", "variable_code"),
        primary_key=True,
    )
    quality_score = Column(Integer)
    imputation = Column(String(50))
    extrapolation_ranges = Column(Text)
