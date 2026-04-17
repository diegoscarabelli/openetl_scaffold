"""
SQLAlchemy ORM models for the WID pipeline.

Keep in sync with tables.ddl: same column names, types, and constraints.
"""

from sqlalchemy import (
    Column,
    Integer,
    Numeric,
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

    country_code = Column(Text, primary_key=True)
    name = Column(Text)
    # create_ts TIMESTAMPTZ is managed by the database default.
    # update_ts TIMESTAMPTZ is added automatically by UpsertBase.


class Variable(UpsertBase):
    """
    WID variable dimension (sixlet codes).
    """

    __tablename__ = "variable"

    variable_code = Column(Text, primary_key=True)
    concept = Column(Text, nullable=False)
    series_type = Column(Text, nullable=False)
    short_name = Column(Text, nullable=False)
    description = Column(Text)
    technical_description = Column(Text)
    unit = Column(Text, nullable=False)
    population_type = Column(Text)
    age_group = Column(Text)


class Percentile(UpsertBase):
    """
    Distribution percentile dimension.
    """

    __tablename__ = "percentile"

    percentile_code = Column(Text, primary_key=True)
    lower_bound = Column(Numeric(6, 2), nullable=False)
    upper_bound = Column(Numeric(6, 2), nullable=False)
    width = Column(Numeric(6, 2), nullable=False)
    granularity = Column(Text, nullable=False)


class Observation(UpsertBase):
    """
    Fact table: distribution values per country/variable/percentile/year.
    """

    __tablename__ = "observation"

    country_code = Column(
        Text,
        fkey("wid", "country", "country_code"),
        primary_key=True,
    )
    variable_code = Column(
        Text,
        fkey("wid", "variable", "variable_code"),
        primary_key=True,
    )
    percentile_code = Column(
        Text,
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
        Text,
        fkey("wid", "country", "country_code"),
        primary_key=True,
    )
    variable_code = Column(
        Text,
        fkey("wid", "variable", "variable_code"),
        primary_key=True,
    )
    quality_score = Column(Integer)
    imputation = Column(Text)
    extrapolation_ranges = Column(Text)
