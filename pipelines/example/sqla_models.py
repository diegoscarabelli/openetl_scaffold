"""SQLAlchemy ORM models for the example pipeline.

Keep in sync with tables.ddl: same column names, types, and constraints.
"""
from sqlalchemy import Column, Integer, Text

from lib.sql_utils import make_base

# Tables with upsert semantics — update_ts is injected automatically.
UpsertBase = make_base(schema="example", include_update_ts=True)


class ExampleRecord(UpsertBase):
    """Replace with your domain entity (e.g. Connection, Activity)."""

    __tablename__ = "example_record"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(Text, nullable=False, unique=True)  # business / natural key
    value = Column(Text)
    # update_ts TIMESTAMPTZ is added automatically by UpsertBase
