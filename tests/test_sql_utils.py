"""
Unit tests for lib.sql_utils module.

This test suite covers:
    - make_base() declarative base creation with schema and timestamps.
    - upsert_model_instances() for inserting and updating ORM instances.
    - _upsert_values() low-level upsert with RETURNING and INSERT_IGNORE.
    - QueryType dispatch logic.
    - Conflict handling (update, do-nothing).
    - latest_check_column conditional updates.
"""

import pytest
from sqlalchemy import Column, Integer, String, delete, func, select, text
from sqlalchemy.orm import DeclarativeBase

from lib.sql_utils import QueryType, _upsert_values, make_base, upsert_model_instances
from tests.conftest import MyTest

# -----------------------------------------------------------------------
# TestMakeBase
# -----------------------------------------------------------------------


class TestMakeBase:
    """
    Tests for make_base() declarative base factory.
    """

    def test_make_base_with_schema(self) -> None:
        """
        Test that make_base creates a base scoped to the given schema.
        """

        Base = make_base(schema="test_schema")

        class TestModel(Base):
            __tablename__ = "test_table"
            id = Column(Integer, primary_key=True)
            name = Column(String)

        assert TestModel.__table__.schema == "test_schema"

    def test_make_base_without_schema(self) -> None:
        """
        Test that make_base works without a schema.
        """

        Base = make_base()

        class TestModel(Base):
            __tablename__ = "test_no_schema"
            id = Column(Integer, primary_key=True)

        assert TestModel.__table__.schema is None

    def test_make_base_with_update_ts(self) -> None:
        """
        Test that make_base injects update_ts and create_ts columns.
        """

        Base = make_base(schema="ts_test_schema", include_update_ts=True)

        class TestModel(Base):
            __tablename__ = "test_ts_table"
            id = Column(Integer, primary_key=True)
            name = Column(String)

        assert hasattr(TestModel, "update_ts")
        assert hasattr(TestModel, "create_ts")

    def test_make_base_without_update_ts(self) -> None:
        """
        Test that make_base omits update_ts but keeps create_ts.
        """

        Base = make_base(schema="no_ts_test_schema", include_update_ts=False)

        class TestModel(Base):
            __tablename__ = "test_no_ts_table"
            id = Column(Integer, primary_key=True)
            name = Column(String)

        assert not hasattr(TestModel, "update_ts")
        assert hasattr(TestModel, "create_ts")

    def test_make_base_shared_metadata(self) -> None:
        """
        Test that passing a MetaData instance shares it across bases.
        """

        from sqlalchemy import MetaData

        meta = MetaData()
        Base = make_base(metadata=meta)

        class TestModel(Base):
            __tablename__ = "test_shared_meta"
            id = Column(Integer, primary_key=True)

        assert TestModel.metadata is meta


# -----------------------------------------------------------------------
# TestUpsertModelInstances
# -----------------------------------------------------------------------


class TestUpsertModelInstances:
    """
    Tests for upsert_model_instances() bulk operations.
    """

    @staticmethod
    def set_up_tbl(session) -> None:
        """
        Add a row with id=1 and col_a="A" to the session and flush.
        """

        obj = MyTest(id=1, col_a="A")
        session.add(obj)
        session.flush()

    def test_insert_new_rows(self, db_session) -> None:
        """
        Test that upsert_model_instances inserts new rows.
        """

        obj1 = MyTest(id=1, col_a="A")
        obj2 = MyTest(id=2, col_a="B")
        upsert_model_instances(
            session=db_session,
            model_instances=[obj1, obj2],
            conflict_columns=["id"],
            on_conflict_update=True,
        )
        rows = db_session.execute(select(MyTest).order_by(MyTest.id)).scalars().all()
        assert len(rows) == 2
        assert rows[0].col_a == "A"
        assert rows[1].col_a == "B"

    def test_upsert_updates_on_conflict(self, db_session) -> None:
        """
        Test that upsert updates existing rows on conflict.
        """

        obj = MyTest(id=1, col_a="A")
        upsert_model_instances(
            session=db_session,
            model_instances=[obj],
            conflict_columns=["id"],
            on_conflict_update=True,
        )

        obj_updated = MyTest(id=1, col_a="B")
        upsert_model_instances(
            session=db_session,
            model_instances=[obj_updated],
            conflict_columns=["id"],
            on_conflict_update=True,
        )
        row = db_session.execute(select(MyTest).where(MyTest.id == 1)).scalars().first()
        assert row.col_a == "B"

    def test_do_nothing_on_conflict(self, db_session) -> None:
        """
        Test that on_conflict_update=False skips conflicting rows.
        """

        obj = MyTest(id=1, col_a="A")
        upsert_model_instances(
            session=db_session,
            model_instances=[obj],
            conflict_columns=["id"],
            on_conflict_update=True,
        )

        obj_dup = MyTest(id=1, col_a="C")
        upsert_model_instances(
            session=db_session,
            model_instances=[obj_dup],
            conflict_columns=["id"],
            on_conflict_update=False,
        )
        row = db_session.execute(select(MyTest).where(MyTest.id == 1)).scalars().first()
        assert row.col_a == "A"

    def test_update_columns_selective(self, db_session) -> None:
        """
        Test that update_columns limits which columns are updated.
        """

        obj = MyTest(id=1, col_a="A", col_b="B1")
        upsert_model_instances(
            session=db_session,
            model_instances=[obj],
            conflict_columns=["id"],
            on_conflict_update=True,
        )

        obj_updated = MyTest(id=1, col_a="X", col_b="B2")
        upsert_model_instances(
            session=db_session,
            model_instances=[obj_updated],
            conflict_columns=["id"],
            on_conflict_update=True,
            update_columns=["col_b"],
        )
        row = db_session.execute(select(MyTest).where(MyTest.id == 1)).scalars().first()
        # col_a should NOT be updated; col_b should.
        assert row.col_a == "A"
        assert row.col_b == "B2"

    def test_empty_model_instances_raises(self, db_session) -> None:
        """
        Test that an empty list raises ValueError.
        """

        with pytest.raises(ValueError, match="cannot be empty"):
            upsert_model_instances(
                session=db_session,
                model_instances=[],
                conflict_columns=["id"],
            )

    def test_mixed_types_raises(self, db_session) -> None:
        """
        Test that mixed model types raise TypeError.
        """

        class TempBase(DeclarativeBase):
            pass

        class OtherModel(TempBase):
            __tablename__ = "other"
            id = Column(Integer, primary_key=True)

        with pytest.raises(TypeError, match="same type"):
            upsert_model_instances(
                session=db_session,
                model_instances=[MyTest(id=1, col_a="A"), OtherModel(id=2)],
                conflict_columns=["id"],
            )

    def test_rollback(self, db_session) -> None:
        """
        Test that rollback undoes inserted rows.
        """

        obj1 = MyTest(id=1, col_a="A")
        obj2 = MyTest(id=2, col_a="B")
        upsert_model_instances(
            session=db_session,
            model_instances=[obj1],
            conflict_columns=["id"],
        )
        upsert_model_instances(
            session=db_session,
            model_instances=[obj2],
            conflict_columns=["id"],
        )
        count = db_session.execute(select(func.count()).select_from(MyTest)).scalar()
        assert count == 2
        db_session.rollback()
        db_session.execute(delete(MyTest))
        db_session.commit()
        count = db_session.execute(select(func.count()).select_from(MyTest)).scalar()
        assert count == 0

    def test_latest_check_column_strict_greater(self, db_session) -> None:
        """
        Test latest_check_column with strict > comparison.
        """

        # Create temp table with version column.
        db_session.execute(
            text(
                "CREATE TEMP TABLE test_version_strict ("
                "id INTEGER PRIMARY KEY,"
                " name TEXT,"
                " version INTEGER NOT NULL)"
            )
        )

        class TempBase(DeclarativeBase):
            pass

        class TempModel(TempBase):
            __tablename__ = "test_version_strict"
            id = Column(Integer, primary_key=True)
            name = Column(String)
            version = Column(Integer)

        # Insert with version=5.
        upsert_model_instances(
            session=db_session,
            model_instances=[TempModel(id=1, name="initial", version=5)],
            conflict_columns=["id"],
            on_conflict_update=True,
            latest_check_column="version",
            latest_check_inclusive=False,
        )

        # Same version should NOT update.
        upsert_model_instances(
            session=db_session,
            model_instances=[TempModel(id=1, name="same_version", version=5)],
            conflict_columns=["id"],
            on_conflict_update=True,
            latest_check_column="version",
            latest_check_inclusive=False,
        )
        row = (
            db_session.execute(select(TempModel).where(TempModel.id == 1))
            .scalars()
            .first()
        )
        assert row.name == "initial"

        # Greater version should update.
        upsert_model_instances(
            session=db_session,
            model_instances=[TempModel(id=1, name="newer_version", version=6)],
            conflict_columns=["id"],
            on_conflict_update=True,
            latest_check_column="version",
            latest_check_inclusive=False,
        )
        # Expire stale ORM identity map after Core-level upsert.
        db_session.expire_all()
        row = (
            db_session.execute(select(TempModel).where(TempModel.id == 1))
            .scalars()
            .first()
        )
        assert row.name == "newer_version"
        assert row.version == 6

    def test_latest_check_column_inclusive(self, db_session) -> None:
        """
        Test latest_check_column with >= comparison (inclusive).
        """

        db_session.execute(
            text(
                "CREATE TEMP TABLE test_version_incl ("
                "id INTEGER PRIMARY KEY,"
                " name TEXT,"
                " version INTEGER NOT NULL)"
            )
        )

        class TempBase(DeclarativeBase):
            pass

        class TempModel(TempBase):
            __tablename__ = "test_version_incl"
            id = Column(Integer, primary_key=True)
            name = Column(String)
            version = Column(Integer)

        # Insert with version=5.
        upsert_model_instances(
            session=db_session,
            model_instances=[TempModel(id=1, name="initial", version=5)],
            conflict_columns=["id"],
            on_conflict_update=True,
            latest_check_column="version",
            latest_check_inclusive=True,
        )

        # Same version with inclusive=True SHOULD update.
        upsert_model_instances(
            session=db_session,
            model_instances=[TempModel(id=1, name="same_version_updated", version=5)],
            conflict_columns=["id"],
            on_conflict_update=True,
            latest_check_column="version",
            latest_check_inclusive=True,
        )
        row = (
            db_session.execute(select(TempModel).where(TempModel.id == 1))
            .scalars()
            .first()
        )
        assert row.name == "same_version_updated"
        assert row.version == 5

    def test_returning_columns_on_upsert(self, db_session) -> None:
        """
        Test that returning_columns returns model instances after upsert.
        """

        obj = MyTest(id=1, col_a="A", col_b="B1")
        results = upsert_model_instances(
            session=db_session,
            model_instances=[obj],
            conflict_columns=["id"],
            on_conflict_update=True,
            returning_columns=["id", "col_a"],
        )
        assert results is not None
        assert len(results) == 1
        assert results[0].id == 1
        assert results[0].col_a == "A"

    def test_returning_columns_none_returns_none(self, db_session) -> None:
        """
        Test that omitting returning_columns returns None.
        """

        obj = MyTest(id=1, col_a="A")
        result = upsert_model_instances(
            session=db_session,
            model_instances=[obj],
            conflict_columns=["id"],
            on_conflict_update=True,
        )
        assert result is None

    def test_insert_no_conflict_columns(self, db_session) -> None:
        """
        Test plain INSERT (no conflict_columns, no on_conflict_update).
        """

        obj = MyTest(id=1, col_a="A")
        upsert_model_instances(
            session=db_session,
            model_instances=[obj],
        )
        row = db_session.execute(select(MyTest).where(MyTest.id == 1)).scalars().first()
        assert row.col_a == "A"

    def test_insert_with_returning(self, db_session) -> None:
        """
        Test plain INSERT with returning_columns.
        """

        obj = MyTest(id=1, col_a="A")
        results = upsert_model_instances(
            session=db_session,
            model_instances=[obj],
            returning_columns=["id", "col_a"],
        )
        assert results is not None
        assert len(results) == 1
        assert results[0].id == 1

    def test_insert_ignore_with_returning(self, db_session) -> None:
        """
        Test INSERT_IGNORE with returning_columns re-queries existing rows.
        """

        # Insert initial row.
        obj1 = MyTest(id=1, col_a="A")
        upsert_model_instances(
            session=db_session,
            model_instances=[obj1],
            conflict_columns=["id"],
            on_conflict_update=True,
        )

        # INSERT_IGNORE with returning should still return the existing row.
        obj_dup = MyTest(id=1, col_a="IGNORED")
        results = upsert_model_instances(
            session=db_session,
            model_instances=[obj_dup],
            conflict_columns=["id"],
            on_conflict_update=False,
            returning_columns=["id", "col_a"],
        )
        assert results is not None
        assert len(results) == 1
        # The original value should be returned, not the ignored one.
        assert results[0].col_a == "A"

    def test_upsert_requires_conflict_columns(self, db_session) -> None:
        """
        Test that on_conflict_update=True without conflict_columns raises.
        """

        obj = MyTest(id=1, col_a="A")
        with pytest.raises(ValueError, match="conflict_columns"):
            upsert_model_instances(
                session=db_session,
                model_instances=[obj],
                on_conflict_update=True,
            )


# -----------------------------------------------------------------------
# TestUpsertValues
# -----------------------------------------------------------------------


class TestUpsertValues:
    """
    Tests for _upsert_values() low-level upsert function.
    """

    def test_upsert_values_returns_dicts(self, db_session) -> None:
        """
        Test that _upsert_values returns dicts when returning_columns given.
        """

        values = [{"id": 1, "col_a": "A"}]
        results = _upsert_values(
            model=MyTest,
            values=values,
            session=db_session,
            conflict_columns=["id"],
            on_conflict_update=True,
            returning_columns=["id", "col_a"],
        )
        assert results is not None
        assert isinstance(results[0], dict)
        assert results[0]["id"] == 1
        assert results[0]["col_a"] == "A"

    def test_upsert_values_returns_none_without_returning(self, db_session) -> None:
        """
        Test that _upsert_values returns None without returning_columns.
        """

        values = [{"id": 1, "col_a": "A"}]
        result = _upsert_values(
            model=MyTest,
            values=values,
            session=db_session,
            conflict_columns=["id"],
            on_conflict_update=True,
        )
        assert result is None

    def test_query_type_dispatch_insert(self, db_session) -> None:
        """
        Test that no conflict_columns and on_conflict_update=False yields INSERT.
        """

        values = [{"id": 1, "col_a": "A"}]
        _upsert_values(
            model=MyTest,
            values=values,
            session=db_session,
        )
        row = db_session.execute(select(MyTest).where(MyTest.id == 1)).scalars().first()
        assert row.col_a == "A"

    def test_query_type_dispatch_insert_ignore(self, db_session) -> None:
        """
        Test INSERT_IGNORE: conflict_columns set, on_conflict_update=False.
        """

        _upsert_values(
            model=MyTest,
            values=[{"id": 1, "col_a": "A"}],
            session=db_session,
            conflict_columns=["id"],
            on_conflict_update=False,
        )
        # Same id, different value: should be ignored.
        _upsert_values(
            model=MyTest,
            values=[{"id": 1, "col_a": "B"}],
            session=db_session,
            conflict_columns=["id"],
            on_conflict_update=False,
        )
        row = db_session.execute(select(MyTest).where(MyTest.id == 1)).scalars().first()
        assert row.col_a == "A"


# -----------------------------------------------------------------------
# TestChunkedUpsert
# -----------------------------------------------------------------------


class TestChunkedUpsert:
    """
    Tests for multi-chunk execution in _upsert_values().

    Uses a tiny chunk_size to force multiple INSERT statements and verifies that rows
    and RETURNING results are aggregated correctly across chunks.
    """

    def test_upsert_all_rows_inserted_across_chunks(self, db_session) -> None:
        """
        Test that all rows are inserted when split across chunks.
        """

        values = [{"id": i, "col_a": f"val_{i}"} for i in range(1, 6)]
        _upsert_values(
            model=MyTest,
            values=values,
            session=db_session,
            conflict_columns=["id"],
            on_conflict_update=True,
            chunk_size=2,
        )
        count = db_session.execute(select(func.count()).select_from(MyTest)).scalar()
        assert count == 5

    def test_upsert_returning_aggregated_across_chunks(self, db_session) -> None:
        """
        Test that RETURNING results are collected from every chunk.
        """

        values = [{"id": i, "col_a": f"val_{i}"} for i in range(1, 6)]
        results = _upsert_values(
            model=MyTest,
            values=values,
            session=db_session,
            conflict_columns=["id"],
            on_conflict_update=True,
            returning_columns=["id", "col_a"],
            chunk_size=2,
        )
        assert results is not None
        assert len(results) == 5
        returned_ids = sorted(r["id"] for r in results)
        assert returned_ids == [1, 2, 3, 4, 5]

    def test_upsert_updates_existing_across_chunks(self, db_session) -> None:
        """
        Test that chunked upsert updates existing rows correctly.
        """

        # Seed rows.
        seed = [{"id": i, "col_a": f"old_{i}"} for i in range(1, 6)]
        _upsert_values(
            model=MyTest,
            values=seed,
            session=db_session,
            conflict_columns=["id"],
            on_conflict_update=True,
        )

        # Upsert with new values in small chunks.
        updated = [{"id": i, "col_a": f"new_{i}"} for i in range(1, 6)]
        _upsert_values(
            model=MyTest,
            values=updated,
            session=db_session,
            conflict_columns=["id"],
            on_conflict_update=True,
            chunk_size=2,
        )
        rows = db_session.execute(select(MyTest).order_by(MyTest.id)).scalars().all()
        assert all(r.col_a == f"new_{r.id}" for r in rows)

    def test_insert_ignore_returning_across_chunks(self, db_session) -> None:
        """
        Test INSERT_IGNORE with RETURNING re-queries per chunk.
        """

        # Seed a subset of rows.
        seed = [{"id": 1, "col_a": "A"}, {"id": 3, "col_a": "C"}]
        _upsert_values(
            model=MyTest,
            values=seed,
            session=db_session,
            conflict_columns=["id"],
            on_conflict_update=True,
        )

        # INSERT_IGNORE with mix of new and existing rows.
        values = [{"id": i, "col_a": f"new_{i}"} for i in range(1, 6)]
        results = _upsert_values(
            model=MyTest,
            values=values,
            session=db_session,
            conflict_columns=["id"],
            on_conflict_update=False,
            returning_columns=["id", "col_a"],
            chunk_size=2,
        )
        assert results is not None
        assert len(results) == 5
        by_id = {r["id"]: r["col_a"] for r in results}
        # Existing rows keep original values.
        assert by_id[1] == "A"
        assert by_id[3] == "C"
        # New rows have the inserted values.
        assert by_id[2] == "new_2"
        assert by_id[4] == "new_4"
        assert by_id[5] == "new_5"

    def test_plain_insert_across_chunks(self, db_session) -> None:
        """
        Test plain INSERT (no conflict) works across chunks.
        """

        values = [{"id": i, "col_a": f"val_{i}"} for i in range(1, 6)]
        _upsert_values(
            model=MyTest,
            values=values,
            session=db_session,
            chunk_size=2,
        )
        count = db_session.execute(select(func.count()).select_from(MyTest)).scalar()
        assert count == 5


# -----------------------------------------------------------------------
# TestQueryType
# -----------------------------------------------------------------------


class TestQueryType:
    """
    Tests for QueryType enumeration.
    """

    def test_query_type_values(self) -> None:
        """
        Test that QueryType has expected string values.
        """

        assert QueryType.UPSERT == "upsert"
        assert QueryType.INSERT == "insert"
        assert QueryType.INSERT_IGNORE == "insert_ignore"
