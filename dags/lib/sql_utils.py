"""
Database utilities for working with SQLAlchemy ORM models and PostgreSQL databases.

This module provides database interaction capabilities including engine creation,
custom ORM base classes, foreign key helpers, and bulk operations. It includes:
    - get_engine() for creating engines from environment variables.
    - make_base() for schema-scoped declarative bases with optional timestamps.
    - fkey() for fully-qualified foreign key references.
    - upsert_model_instances() for bulk INSERT ... ON CONFLICT operations.
    - _upsert_values() low-level upsert with RETURNING, INSERT_IGNORE, and
      auto update_ts injection.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Type

from dotenv import load_dotenv
from sqlalchemy import DateTime, ForeignKey, MetaData, select
from sqlalchemy import create_engine as _create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    declared_attr,
    mapped_column,
)
from sqlalchemy.sql import and_, or_

load_dotenv()


class QueryType:
    """
    Enumeration of query types for upsert operations.
    """

    UPSERT = "upsert"
    INSERT = "insert"
    INSERT_IGNORE = "insert_ignore"


def get_engine(schema: Optional[str] = None) -> Engine:
    """
    Create a SQLAlchemy engine from environment variables.

    Reads SQL_DB_HOST, SQL_DB_PORT, SQL_DB_NAME, SQL_DB_USER, SQL_DB_PASSWORD from .env
    (or the environment). Set DATABASE_URL to override all individual vars. The SQL_DB_
    prefix avoids conflicts with Airflow's entrypoint script, which reserves DB_HOST for
    health-check logic.

    :param schema: PostgreSQL schema to set as the default search path. If None, uses
        the database default search path.
    :return: A configured SQLAlchemy Engine.
    """

    load_dotenv()
    url = os.getenv("DATABASE_URL") or (
        "postgresql+psycopg://{user}:{password}@{host}:{port}/{db}".format(
            user=os.environ["SQL_DB_USER"],
            password=os.environ["SQL_DB_PASSWORD"],
            host=os.getenv("SQL_DB_HOST", "localhost"),
            port=os.getenv("SQL_DB_PORT", "5432"),
            db=os.environ["SQL_DB_NAME"],
        )
    )
    connect_args: Dict[str, Any] = {}
    if schema:
        connect_args["options"] = f"-csearch_path={schema},public"
    return _create_engine(url, connect_args=connect_args)


def make_base(
    schema: Optional[str] = None,
    include_update_ts: bool = False,
    metadata: Optional[MetaData] = None,
) -> Type:
    """
    Create a custom base class for SQLAlchemy ORM models.

    :param schema: Schema name for the SQL database table.
    :param include_update_ts: Whether to include an update timestamp column.
    :param metadata: SQLAlchemy MetaData instance to share across models.
    :return: Declarative base class for ORM models.
    """

    _metadata = metadata or MetaData()

    class _Base(DeclarativeBase):
        metadata = _metadata  # type: ignore[assignment]

    class _CustomBase(_Base):
        __abstract__ = True

        @declared_attr
        def create_ts(cls) -> Mapped[datetime]:
            return mapped_column(
                DateTime(timezone=True),
                default=datetime.now,
                nullable=False,
            )

    if include_update_ts:

        class _CustomBaseWithTs(_CustomBase):
            __abstract__ = True

            @declared_attr
            def update_ts(cls) -> Mapped[datetime]:
                return mapped_column(
                    DateTime(timezone=True),
                    default=datetime.now,
                    onupdate=datetime.now,
                    nullable=False,
                )

        result_base = _CustomBaseWithTs
    else:
        result_base = _CustomBase

    if schema:
        result_base.__table_args__ = {"schema": schema}

    return result_base


def fkey(
    schema: str,
    table_name: str,
    column_name: Optional[str] = None,
) -> ForeignKey:
    """
    Generate a ForeignKey object for a table in the specified schema.

    :param schema: Schema name.
    :param table_name: Foreign table name.
    :param column_name: Foreign column name, defaults to <table_name>_id.
    :return: ForeignKey object.
    """

    return ForeignKey(".".join([schema, table_name, column_name or f"{table_name}_id"]))


def upsert_model_instances(
    session: Session,
    model_instances: List[Any],
    update_columns: Optional[List[str]] = None,
    conflict_columns: Optional[List[str]] = None,
    on_conflict_update: bool = False,
    latest_check_column: Optional[str] = None,
    latest_check_inclusive: bool = False,
    returning_columns: Optional[List[str]] = None,
) -> Optional[List[Any]]:
    """
    Bulk upsert ORM instances using PostgreSQL INSERT ... ON CONFLICT.

    Converts model instances to dictionaries, delegates the upsert logic to
    _upsert_values(), and optionally returns the persisted instances as they exist in
    the database after the operation.

    :param session: An open SQLAlchemy Session.
    :param model_instances: ORM instances to insert or update. All instances must be of
        the same model type.
    :param update_columns: Columns to set on conflict. Defaults to all columns except
        the conflict columns.
    :param conflict_columns: Column names forming the unique conflict target. Must match
        a UNIQUE constraint in the DDL. If None, a simple insert is performed.
    :param on_conflict_update: If True, update rows on conflict; if False, ignore
        conflicts and do not update existing rows.
    :param latest_check_column: Only update if incoming value >= (or >) existing.
        Prevents stale data from overwriting newer records.
    :param latest_check_inclusive: Use >= when True, > when False.
    :param returning_columns: Column names to return via RETURNING clause. If None, no
        RETURNING is issued and the function returns None.
    :return: List of model instances (with only the requested columns populated) if
        returning_columns is specified, otherwise None.
    """

    if not model_instances:
        raise ValueError("`model_instances` list cannot be empty.")

    model = model_instances[0].__class__
    if not all(isinstance(inst, model) for inst in model_instances):
        raise TypeError(
            f"All `model_instances` must be of the same type:" f" {model.__name__}."
        )

    model_columns = model.__table__.columns.keys()
    values = []
    for instance in model_instances:
        instance_dict = {
            key: value
            for key, value in instance.__dict__.items()
            if key in model_columns
        }
        values.append(instance_dict)

    results = _upsert_values(
        model=model,
        values=values,
        session=session,
        update_columns=update_columns,
        conflict_columns=conflict_columns,
        on_conflict_update=on_conflict_update,
        latest_check_column=latest_check_column,
        latest_check_inclusive=latest_check_inclusive,
        returning_columns=returning_columns,
    )

    if results is None:
        return None

    return [model(**result) for result in results]


def _upsert_values(
    model: Type,
    values: List[dict],
    session: Session,
    update_columns: Optional[List[str]] = None,
    conflict_columns: Optional[List[str]] = None,
    on_conflict_update: bool = False,
    latest_check_column: Optional[str] = None,
    latest_check_inclusive: bool = False,
    returning_columns: Optional[List[str]] = None,
) -> Optional[List[Dict[str, Any]]]:
    """
    Bulk upsert dictionaries into a PostgreSQL table via SQLAlchemy Core.

    Builds and executes the appropriate SQL statement for insert, upsert, or insert-
    ignore, and can return the resulting rows as dictionaries if requested.

    :param model: SQLAlchemy ORM model class representing the table.
    :param values: List of dicts mapping column names to values.
    :param session: An open SQLAlchemy Session.
    :param update_columns: Columns to update on conflict. Defaults to all columns except
        the conflict columns.
    :param conflict_columns: Columns forming the unique conflict target. If None, a
        simple insert is performed.
    :param on_conflict_update: If True, update rows on conflict; if False, ignore
        conflicts.
    :param latest_check_column: Only update when the incoming value is greater than (or
        >=) the existing value.
    :param latest_check_inclusive: Use >= when True, > when False.
    :param returning_columns: Column names to return via RETURNING. If None, no
        RETURNING is issued.
    :return: List of dicts with returned values if returning_columns is specified,
        otherwise None.
    """

    if on_conflict_update:
        if not conflict_columns:
            raise ValueError(
                "`conflict_columns` must be specified if"
                " `on_conflict_update` is True."
            )
        query_type = QueryType.UPSERT
    else:
        query_type = (
            QueryType.INSERT_IGNORE
            if conflict_columns is not None
            else QueryType.INSERT
        )

    conflict_columns = conflict_columns or []
    returned_values: List[Dict[str, Any]] = []

    if update_columns is None:
        update_columns = [
            col.name
            for col in model.__table__.columns
            if col.name not in conflict_columns
        ]

    insert_stmt = insert(model).values(values)

    if query_type == QueryType.UPSERT:
        update_dict = {col: insert_stmt.excluded[col] for col in update_columns}

        # Automatically inject update_ts if the model has it.
        if hasattr(model, "update_ts") and "update_ts" not in update_dict:
            update_dict["update_ts"] = datetime.now(tz=timezone.utc)

        if latest_check_column:
            excluded_col = insert_stmt.excluded[latest_check_column]
            existing_col = getattr(model, latest_check_column)
            where_clause = (
                excluded_col >= existing_col
                if latest_check_inclusive
                else excluded_col > existing_col
            )
        else:
            where_clause = None

        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=conflict_columns,
            set_=update_dict,
            where=where_clause,
        )

        if returning_columns:
            upsert_stmt = upsert_stmt.returning(
                *[getattr(model, col) for col in returning_columns]
            )

    elif query_type == QueryType.INSERT:
        upsert_stmt = insert_stmt

        if returning_columns:
            upsert_stmt = upsert_stmt.returning(
                *[getattr(model, col) for col in returning_columns]
            )

    elif query_type == QueryType.INSERT_IGNORE:
        upsert_stmt = insert_stmt.on_conflict_do_nothing(
            index_elements=conflict_columns
        )

    else:
        raise ValueError(f"Invalid query type: {query_type}.")

    # Execute and flush (no commit). Sends SQL to the database within
    # the current transaction. Requires explicit session.commit() by
    # the caller.
    result = session.execute(upsert_stmt)
    session.flush()

    if returning_columns:
        if query_type in [QueryType.UPSERT, QueryType.INSERT]:
            returned_values.extend([row._asdict() for row in result.fetchall()])

        elif query_type == QueryType.INSERT_IGNORE:
            # INSERT ... ON CONFLICT DO NOTHING does not return
            # ignored rows. Re-query to get all matching rows.
            conflict_conditions = [
                and_(
                    *[
                        (
                            getattr(model, col) == value[col]
                            if value[col] is not None
                            else getattr(model, col).is_(None)
                        )
                        for col in conflict_columns
                    ]
                )
                for value in values
            ]
            stmt = select(*[getattr(model, col) for col in returning_columns]).where(
                or_(*conflict_conditions)
            )
            returned_values.extend(
                [row._asdict() for row in session.execute(stmt).all()]
            )

        else:
            raise ValueError(f"Invalid query type: {query_type}.")

    return returned_values if returning_columns else None
