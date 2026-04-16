"""SQLAlchemy helpers: get_engine, make_base, fkey, upsert_model_instances."""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple, Type

from dotenv import load_dotenv
from sqlalchemy import Column, DateTime, ForeignKey, MetaData, func
from sqlalchemy import create_engine as _create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import DeclarativeBase, declared_attr

load_dotenv()

_metadata_registry: Dict[str, MetaData] = {}
_base_registry: Dict[Tuple, Type] = {}


def get_engine(schema: Optional[str] = None):
    """Create a SQLAlchemy engine from environment variables.

    Reads DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD from .env
    (or the environment). Set DATABASE_URL to override all individual vars.
    """
    load_dotenv()
    url = os.getenv("DATABASE_URL") or (
        "postgresql+psycopg://{user}:{password}@{host}:{port}/{db}".format(
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            db=os.environ["DB_NAME"],
        )
    )
    connect_args: Dict[str, Any] = {}
    if schema:
        connect_args["options"] = f"-csearch_path={schema},public"
    return _create_engine(url, connect_args=connect_args)


def make_base(schema: str, include_update_ts: bool = True) -> Type:
    """Create a SQLAlchemy declarative base scoped to a PostgreSQL schema.

    Results are cached — calling make_base with the same arguments always
    returns the same class, so models across a pipeline share one MetaData.

    Args:
        schema: PostgreSQL schema name (e.g. "linkedin").
        include_update_ts: When True, injects an update_ts TIMESTAMPTZ column
                           set to NOW() on every INSERT and UPDATE.

    Usage:
        UpsertBase = make_base(schema="linkedin", include_update_ts=True)
        InsertBase = make_base(schema="linkedin", include_update_ts=False)

        class Connection(UpsertBase):
            __tablename__ = "connection"
            ...
    """
    key = (schema, include_update_ts)
    if key in _base_registry:
        return _base_registry[key]

    if schema not in _metadata_registry:
        _metadata_registry[schema] = MetaData(schema=schema)
    meta = _metadata_registry[schema]

    class _Base(DeclarativeBase):
        metadata = meta  # type: ignore[assignment]

    if not include_update_ts:
        _base_registry[key] = _Base
        return _Base

    class _BaseWithTs(_Base):
        __abstract__ = True

        @declared_attr
        def update_ts(cls) -> Column:
            return Column(
                DateTime(timezone=True),
                server_default=func.now(),
                onupdate=func.now(),
                nullable=False,
            )

    _base_registry[key] = _BaseWithTs
    return _BaseWithTs


def fkey(schema: str, table: str, column: str) -> ForeignKey:
    """Return a fully-qualified ForeignKey (schema.table.column).

    Always use this instead of bare ForeignKey("table.column") — bare
    references silently omit the schema in multi-schema databases.

    Example:
        user_id = Column(BigInteger, fkey("auth", "user", "id"), nullable=False)
    """
    return ForeignKey(f"{schema}.{table}.{column}")


def upsert_model_instances(
    session: Any,
    model_instances: Any,
    conflict_columns: List[str],
    on_conflict_update: bool = True,
    update_columns: Optional[List[str]] = None,
    latest_check_column: Optional[str] = None,
    latest_check_inclusive: bool = True,
) -> None:
    """Bulk upsert ORM instances using PostgreSQL INSERT ... ON CONFLICT.

    Args:
        session: An open SQLAlchemy Session.
        model_instances: ORM instances to insert or update.
        conflict_columns: Column names forming the unique conflict target.
                          Must match a UNIQUE constraint in the DDL.
        on_conflict_update: If False, silently skip conflicting rows.
        update_columns: Columns to set on conflict. Defaults to all
                        non-PK columns not in conflict_columns.
        latest_check_column: Only update if incoming value >= (or >) existing.
                             Prevents stale re-ingested data from overwriting
                             newer records.
        latest_check_inclusive: Use >= when True, > when False.
    """
    if not model_instances:
        return

    model_class = type(model_instances[0])
    table = model_class.__table__  # type: ignore[attr-defined]

    rows = [
        {c.name: getattr(inst, c.name) for c in table.columns}
        for inst in model_instances
    ]
    stmt = insert(table).values(rows)

    if not on_conflict_update:
        stmt = stmt.on_conflict_do_nothing(index_elements=conflict_columns)
    else:
        cols_to_update = update_columns or [
            c.name
            for c in table.columns
            if c.name not in conflict_columns and not c.primary_key
        ]
        set_clause: Dict[str, Any] = {col: stmt.excluded[col] for col in cols_to_update}

        if latest_check_column:
            existing = table.c[latest_check_column]
            incoming = stmt.excluded[latest_check_column]
            cond = incoming >= existing if latest_check_inclusive else incoming > existing
            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_columns, set_=set_clause, where=cond
            )
        else:
            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_columns, set_=set_clause
            )

    session.execute(stmt)
    session.commit()
