"""
Unit tests for lib.sql_utils module.

This test suite covers:
    - make_base() declarative base creation with schema and timestamps.
    - upsert_model_instances() for inserting and updating ORM instances.
    - Conflict handling (update, do-nothing).
    - latest_check_column conditional updates.
"""

from sqlalchemy import Column, Integer, String, text
from sqlalchemy.orm import declarative_base

from lib.sql_utils import make_base, upsert_model_instances
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

        Base = make_base(schema="test_schema", include_update_ts=False)

        class TestModel(Base):
            __tablename__ = "test_table"
            id = Column(Integer, primary_key=True)
            name = Column(String)

        assert TestModel.metadata.schema == "test_schema"

    def test_make_base_with_update_ts(self) -> None:
        """
        Test that make_base injects an update_ts column when requested.
        """

        Base = make_base(schema="ts_test_schema", include_update_ts=True)

        class TestModel(Base):
            __tablename__ = "test_ts_table"
            id = Column(Integer, primary_key=True)
            name = Column(String)

        assert hasattr(TestModel, "update_ts")

    def test_make_base_without_update_ts(self) -> None:
        """
        Test that make_base omits update_ts when include_update_ts=False.
        """

        Base = make_base(schema="no_ts_test_schema", include_update_ts=False)

        class TestModel(Base):
            __tablename__ = "test_no_ts_table"
            id = Column(Integer, primary_key=True)
            name = Column(String)

        assert not hasattr(TestModel, "update_ts")

    def test_make_base_caching(self) -> None:
        """
        Test that make_base returns the same class for identical arguments.
        """

        Base1 = make_base(schema="cache_test", include_update_ts=True)
        Base2 = make_base(schema="cache_test", include_update_ts=True)
        assert Base1 is Base2

    def test_make_base_different_params_different_classes(self) -> None:
        """
        Test that different parameters produce different base classes.
        """

        Base1 = make_base(schema="diff_test", include_update_ts=True)
        Base2 = make_base(schema="diff_test", include_update_ts=False)
        assert Base1 is not Base2


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
        rows = db_session.query(MyTest).order_by(MyTest.id).all()
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
        row = db_session.query(MyTest).filter_by(id=1).first()
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
        row = db_session.query(MyTest).filter_by(id=1).first()
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
        row = db_session.query(MyTest).filter_by(id=1).first()
        # col_a should NOT be updated; col_b should.
        assert row.col_a == "A"
        assert row.col_b == "B2"

    def test_empty_model_instances(self, db_session) -> None:
        """
        Test that an empty list returns without error.
        """

        upsert_model_instances(
            session=db_session,
            model_instances=[],
            conflict_columns=["id"],
        )
        assert db_session.query(MyTest).count() == 0

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
        assert db_session.query(MyTest).count() == 2
        db_session.rollback()
        db_session.query(MyTest).delete()
        db_session.commit()
        assert db_session.query(MyTest).count() == 0

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

        temp_base = declarative_base()

        class TempModel(temp_base):
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
        row = db_session.query(TempModel).filter_by(id=1).first()
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
        row = db_session.query(TempModel).filter_by(id=1).first()
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

        temp_base = declarative_base()

        class TempModel(temp_base):
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
        row = db_session.query(TempModel).filter_by(id=1).first()
        assert row.name == "same_version_updated"
        assert row.version == 5
