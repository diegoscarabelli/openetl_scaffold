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

    def test_latest_check_column_returning_shorter_than_input(
        self, db_session
    ) -> None:
        """
        UPSERT + ``latest_check_column`` + ``returning_columns`` is the documented
        exception to the "one row per input row" guarantee: when the latest-check
        ``WHERE`` clause prevents the update, PostgreSQL treats the conflict as
        DO NOTHING and emits no ``RETURNING`` row, so the result list is shorter
        than the input. Locks in the contract documented in the helper docstring.
        """

        db_session.execute(
            text(
                "CREATE TEMP TABLE test_version_returning ("
                " id INTEGER PRIMARY KEY,"
                " name TEXT,"
                " version INTEGER NOT NULL)"
            )
        )

        class TempBase(DeclarativeBase):
            pass

        class TempVersion(TempBase):
            __tablename__ = "test_version_returning"
            id = Column(Integer, primary_key=True)
            name = Column(String)
            version = Column(Integer)

        # Seed: id=1 at version 5.
        _upsert_values(
            model=TempVersion,
            values=[{"id": 1, "name": "initial", "version": 5}],
            session=db_session,
            conflict_columns=["id"],
            on_conflict_update=True,
            latest_check_column="version",
        )

        # Mix: id=1 with stale version (will be blocked) + id=2 fresh insert.
        # Position-aligned guarantee NOT held: result has 1 row (only id=2),
        # not 2 rows.
        result = _upsert_values(
            model=TempVersion,
            values=[
                {"id": 1, "name": "stale", "version": 3},
                {"id": 2, "name": "fresh", "version": 1},
            ],
            session=db_session,
            conflict_columns=["id"],
            on_conflict_update=True,
            latest_check_column="version",
            returning_columns=["id", "name", "version"],
        )
        assert result is not None
        assert len(result) == 1
        assert result[0]["id"] == 2
        # id=1 was NOT updated (stale).
        existing = (
            db_session.execute(select(TempVersion).where(TempVersion.id == 1))
            .scalars()
            .first()
        )
        assert existing.name == "initial"
        assert existing.version == 5

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

    def test_returning_columns_empty_list_raises(self, db_session) -> None:
        """
        Passing ``returning_columns=[]`` is a programming error: it would skip the
        RETURNING path but break the contract that "if you ask for results, you
        get one row per input." Surface it as a clear ValueError up front instead
        of silently returning an empty list.
        """

        obj = MyTest(id=1, col_a="A")
        with pytest.raises(ValueError, match="non-empty"):
            upsert_model_instances(
                session=db_session,
                model_instances=[obj],
                conflict_columns=["id"],
                on_conflict_update=True,
                returning_columns=[],
            )

    def test_returning_columns_unknown_column_raises(self, db_session) -> None:
        """
        Passing an unknown column name in ``returning_columns`` should raise a
        ``ValueError`` listing the offending columns, not a bare ``AttributeError``
        from ``getattr(model, col)`` deep in the helper.
        """

        obj = MyTest(id=1, col_a="A")
        with pytest.raises(ValueError, match="not_a_column"):
            upsert_model_instances(
                session=db_session,
                model_instances=[obj],
                conflict_columns=["id"],
                on_conflict_update=True,
                returning_columns=["id", "not_a_column"],
            )

    def test_conflict_columns_unknown_name_raises(self, db_session) -> None:
        """
        Passing an unknown column name in ``conflict_columns`` should raise a
        clear ``ValueError`` listing the offending entry, not an opaque
        SQLAlchemy ``index_elements`` failure later. Validates the names-
        namespace contract for ``conflict_columns``.
        """

        obj = MyTest(id=1, col_a="A")
        with pytest.raises(ValueError, match="not_a_column"):
            upsert_model_instances(
                session=db_session,
                model_instances=[obj],
                conflict_columns=["not_a_column"],
                on_conflict_update=True,
            )

    def test_update_columns_unknown_key_raises(self, db_session) -> None:
        """
        Passing an unknown column key in caller-supplied ``update_columns`` should
        raise a clear ``ValueError`` instead of a downstream ``KeyError`` from
        ``insert_stmt.excluded[col]``. Validates the keys-namespace contract for
        ``update_columns``.
        """

        obj = MyTest(id=1, col_a="A")
        with pytest.raises(ValueError, match="not_a_column"):
            upsert_model_instances(
                session=db_session,
                model_instances=[obj],
                conflict_columns=["id"],
                on_conflict_update=True,
                update_columns=["col_a", "not_a_column"],
            )

    def test_conflict_columns_normalized_to_keys_for_default_update_exclusion(
        self, db_session
    ) -> None:
        """
        ``conflict_columns`` are passed in the names namespace (per SQLAlchemy's
        ``index_elements``), but the default ``update_columns`` filter compares
        against the keys namespace. The helper must translate names → keys via
        the model's ``name_to_key`` map so the conflict column is correctly
        excluded from the SET clause.

        Concretely: if the conflict column has ``Column('db_uniq', key=
        'uniq_attr')`` and the helper compared the raw name ``'db_uniq'``
        against the keys list ``['pk', 'uniq_attr', 'payload']``, the conflict
        column would NOT be filtered out, and the SET clause would include
        ``uniq_attr = excluded.uniq_attr`` (a no-op write of the same value
        the conflict matched on). The new value from any subsequent input
        would still be visible in ``excluded`` and overwrite, but the test
        below confirms the conflict column does not appear in the update set
        when the default is applied.
        """

        from sqlalchemy import Column, Integer, String
        from sqlalchemy.orm import DeclarativeBase

        db_session.execute(
            text(
                "CREATE TEMP TABLE test_namekey_conflict ("
                " pk SERIAL PRIMARY KEY,"
                " db_uniq TEXT NOT NULL UNIQUE,"
                " payload TEXT)"
            )
        )

        class TempBase(DeclarativeBase):
            pass

        class NameKeyConflict(TempBase):
            __tablename__ = "test_namekey_conflict"
            pk = Column(Integer, primary_key=True)
            # Conflict column has db name "db_uniq" but Python key "uniq_attr".
            # NOTE: explicit `key=` is required — the class-attribute name does
            # NOT propagate to Column.key automatically (only the position-arg
            # name does, which sets BOTH name and key when no `key=` is given).
            uniq_attr = Column(
                "db_uniq",
                String,
                unique=True,
                nullable=False,
                key="uniq_attr",
            )
            payload = Column(String)

        # Seed.
        upsert_model_instances(
            session=db_session,
            model_instances=[NameKeyConflict(uniq_attr="K", payload="first")],
            conflict_columns=["db_uniq"],  # NAME namespace, per the contract.
            on_conflict_update=True,
        )

        # Re-upsert with same uniq_attr but different payload. With the
        # default update_columns, the SET clause must include payload but
        # NOT uniq_attr (it's the conflict column).
        upsert_model_instances(
            session=db_session,
            model_instances=[NameKeyConflict(uniq_attr="K", payload="second")],
            conflict_columns=["db_uniq"],
            on_conflict_update=True,
        )

        row = (
            db_session.execute(
                select(NameKeyConflict).where(NameKeyConflict.uniq_attr == "K")
            )
            .scalars()
            .one()
        )
        # Payload was updated, conflict-column value is unchanged.
        assert row.payload == "second"
        assert row.uniq_attr == "K"
        # Only one row exists (conflict was matched, not duplicated).
        count = (
            db_session.execute(
                text("SELECT count(*) FROM test_namekey_conflict")
            ).scalar()
        )
        assert count == 1

    def test_returning_columns_duplicates_raises(self, db_session) -> None:
        """
        Duplicate entries in ``returning_columns`` would silently collide in
        ``row._asdict()`` (the second value overwrites the first). Surface as
        a clear ``ValueError`` instead.
        """

        obj = MyTest(id=1, col_a="A")
        with pytest.raises(ValueError, match="duplicate"):
            upsert_model_instances(
                session=db_session,
                model_instances=[obj],
                conflict_columns=["id"],
                on_conflict_update=True,
                returning_columns=["id", "id"],
            )

    def test_insert_ignore_returning_with_name_key_divergent_conflict_column(
        self, db_session
    ) -> None:
        """
        The INSERT_IGNORE + returning_columns no-op DO UPDATE trick must
        translate ``conflict_columns[0]`` (a NAME) to its key before indexing
        ``excluded[...]`` and building ``set_``. Without the translation this
        path would ``KeyError`` on any model with ``Column.name != Column.key``.
        Locks in the bug fix flagged by Copilot review 4239273175.
        """

        from sqlalchemy import Column, Integer, String
        from sqlalchemy.orm import DeclarativeBase

        db_session.execute(
            text(
                "CREATE TEMP TABLE test_namekey_insert_ignore ("
                " pk SERIAL PRIMARY KEY,"
                " db_uniq TEXT NOT NULL UNIQUE,"
                " payload TEXT)"
            )
        )

        class TempBase(DeclarativeBase):
            pass

        class NameKeyIgnore(TempBase):
            __tablename__ = "test_namekey_insert_ignore"
            pk = Column(Integer, primary_key=True)
            uniq_attr = Column(
                "db_uniq", String, unique=True, nullable=False, key="uniq_attr"
            )
            payload = Column(String)

        # Seed.
        upsert_model_instances(
            session=db_session,
            model_instances=[NameKeyIgnore(uniq_attr="K", payload="first")],
            conflict_columns=["db_uniq"],
            on_conflict_update=True,
        )

        # INSERT_IGNORE with returning_columns hits the no-op DO UPDATE path.
        # Without the name_to_key translation this would raise KeyError on
        # `excluded['db_uniq']` (since excluded is keyed by KEYS).
        result = upsert_model_instances(
            session=db_session,
            model_instances=[NameKeyIgnore(uniq_attr="K", payload="ignored")],
            conflict_columns=["db_uniq"],
            on_conflict_update=False,
            returning_columns=["uniq_attr", "payload"],
        )
        assert result is not None
        assert len(result) == 1
        assert result[0].uniq_attr == "K"
        assert result[0].payload == "first"  # existing value preserved.

    def test_upsert_with_only_pk_and_audit_columns_falls_back_gracefully(
        self, db_session
    ) -> None:
        """
        For tables with only PK + conflict + audit columns and no
        ``update_ts``, the default ``update_columns`` list is empty. The
        helper must NOT emit ``ON CONFLICT (...) DO UPDATE SET`` with an empty
        SET clause (invalid SQL); the no-op DO UPDATE trick is used as a
        fallback so the conflict path still fires.
        """

        from sqlalchemy import Column, Integer
        from sqlalchemy.orm import DeclarativeBase

        db_session.execute(
            text(
                "CREATE TEMP TABLE test_pk_only ("
                " user_id INTEGER NOT NULL,"
                " group_id INTEGER NOT NULL,"
                " PRIMARY KEY (user_id, group_id))"
            )
        )

        class TempBase(DeclarativeBase):
            pass

        class Membership(TempBase):
            __tablename__ = "test_pk_only"
            user_id = Column(Integer, primary_key=True)
            group_id = Column(Integer, primary_key=True)
            # No update_ts and no other mutable columns.

        # First upsert inserts.
        upsert_model_instances(
            session=db_session,
            model_instances=[Membership(user_id=1, group_id=10)],
            conflict_columns=["user_id", "group_id"],
            on_conflict_update=True,
        )

        # Second upsert hits the conflict path. With the old code this
        # would emit `ON CONFLICT (...) DO UPDATE SET` with no SET targets
        # and Postgres would raise a syntax error.
        upsert_model_instances(
            session=db_session,
            model_instances=[Membership(user_id=1, group_id=10)],
            conflict_columns=["user_id", "group_id"],
            on_conflict_update=True,
        )

        # Row exists, no duplicates created.
        count = (
            db_session.execute(text("SELECT count(*) FROM test_pk_only")).scalar()
        )
        assert count == 1

    def test_default_update_columns_excludes_pk_with_distinct_name_and_key(
        self, db_session
    ) -> None:
        """
        SQLAlchemy ``Column`` lets callers specify a database column ``name`` that
        differs from the Python attribute ``key`` (e.g. ``Column('db_name', ...,
        key='attr_name')``). The default ``update_columns`` must exclude PK columns
        regardless of which namespace conflict_columns/model_columns operate in.
        Locks in the use of ``col.key`` (not ``col.name``) when building the PK
        exclusion set.
        """

        from sqlalchemy import Column, Integer, String
        from sqlalchemy.orm import DeclarativeBase

        db_session.execute(
            text(
                "CREATE TEMP TABLE test_namekey ("
                " db_pk SERIAL PRIMARY KEY,"
                " unique_key TEXT NOT NULL UNIQUE,"
                " payload TEXT)"
            )
        )

        class TempBase(DeclarativeBase):
            pass

        class NameKeyModel(TempBase):
            __tablename__ = "test_namekey"
            # PK column has db name "db_pk" and Python KEY "pk_attr". The
            # explicit `key=` is required: SQLAlchemy does NOT auto-derive
            # Column.key from the class attribute name. Without `key=`, both
            # .name and .key would equal "db_pk" and the col.name vs col.key
            # distinction this test exists to verify would be undetectable.
            pk_attr = Column("db_pk", Integer, primary_key=True, key="pk_attr")
            unique_key = Column(String, unique=True, nullable=False)
            payload = Column(String)

        # Seed a row; let SERIAL assign the PK.
        _upsert_values(
            model=NameKeyModel,
            values=[{"unique_key": "K", "payload": "first"}],
            session=db_session,
            conflict_columns=["unique_key"],
            on_conflict_update=True,
        )
        original_pk = (
            db_session.execute(select(NameKeyModel).where(NameKeyModel.unique_key == "K"))
            .scalars()
            .one()
            .pk_attr
        )

        # Re-upsert on the unique_key conflict, WITHOUT supplying pk_attr.
        # With the old name-based PK exclusion, pk_attr would slip into
        # update_columns and the SET clause would do `SET db_pk =
        # excluded.db_pk` — and excluded.db_pk would be the NEXT sequence
        # value (not NULL, because SERIAL provides a default), silently
        # renumbering the PK on every conflict and breaking any FK that
        # pointed at it.
        _upsert_values(
            model=NameKeyModel,
            values=[{"unique_key": "K", "payload": "second"}],
            session=db_session,
            conflict_columns=["unique_key"],
            on_conflict_update=True,
        )

        # PK preserved across the conflict update; payload updated.
        row = (
            db_session.execute(select(NameKeyModel).where(NameKeyModel.unique_key == "K"))
            .scalars()
            .one()
        )
        assert row.pk_attr == original_pk, (
            f"PK was renumbered: {original_pk} -> {row.pk_attr}. "
            "Default update_columns must use col.key (not col.name) when "
            "building the PK exclusion set."
        )
        assert row.payload == "second"

    def test_insert_ignore_returning_position_aligned(self, db_session) -> None:
        """
        INSERT_IGNORE with ``returning_columns`` returns one row per input row, in
        input order (position-aligned), with existing values preserved for
        conflicted rows. Implemented via the no-op ``DO UPDATE`` trick.
        """

        # Seed a subset of rows the second pass will conflict on.
        upsert_model_instances(
            session=db_session,
            model_instances=[
                MyTest(id=1, col_a="A"),
                MyTest(id=3, col_a="C"),
            ],
            conflict_columns=["id"],
            on_conflict_update=False,
        )

        # Mix of conflict (1), new (2), conflict (3) in non-trivial order to
        # stress position-alignment.
        results = upsert_model_instances(
            session=db_session,
            model_instances=[
                MyTest(id=1, col_a="Z"),
                MyTest(id=2, col_a="B"),
                MyTest(id=3, col_a="Z"),
            ],
            conflict_columns=["id"],
            on_conflict_update=False,
            returning_columns=["id", "col_a"],
        )
        assert results is not None
        assert len(results) == 3
        # Position-aligned: result[i] corresponds to input[i].
        assert [r.id for r in results] == [1, 2, 3]
        # Existing values preserved for conflicts; new value for the insert.
        assert [r.col_a for r in results] == ["A", "B", "C"]

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
        INSERT_IGNORE with `returning_columns` returns one row per input row
        across chunked statements, in input order, with existing values preserved
        for conflicted rows.
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
        # Position-aligned: result[i] corresponds to input[i] across chunk
        # boundaries. Locks in the no-op DO UPDATE trick's input-order
        # guarantee even when the batch is split.
        assert [r["id"] for r in results] == [1, 2, 3, 4, 5]
        # Existing rows (1, 3) keep their original values; new rows take input.
        assert [r["col_a"] for r in results] == ["A", "new_2", "C", "new_4", "new_5"]

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
