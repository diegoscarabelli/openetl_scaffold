"""
Test fixtures for the scaffold test suite.

Provides database engine and session fixtures backed by a local PostgreSQL instance.
Configure the connection via the TEST_DB_URL environment variable or accept the default
(postgres:postgres@localhost:5432/postgres).
"""

import os
from typing import Generator

import pytest
from dotenv import load_dotenv
from sqlalchemy import Boolean, Column, Integer, String, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session

# Load environment variables from .env file.
load_dotenv()

TEST_DB_URL = os.getenv(
    "TEST_DB_URL",
    "postgresql+psycopg://postgres:postgres@localhost:5432/postgres",
)


class Base(DeclarativeBase):
    """
    Declarative base for test ORM models.
    """


class MyTest(Base):
    """
    SQLAlchemy ORM model for the test table 'sql_utils_test_tbl'.
    """

    __tablename__ = "sql_utils_test_tbl"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, primary_key=True)
    col_a = Column(String, nullable=False)
    col_b = Column(String)
    col_c = Column(Integer)
    latest = Column(Boolean, server_default=text("false"))

    def __eq__(self, other: object) -> bool:
        """
        Compare two MyTest instances for equality.
        """

        if not isinstance(other, MyTest):
            return NotImplemented
        return (
            self.id == other.id
            and self.col_a == other.col_a
            and self.col_b == other.col_b
            and self.col_c == other.col_c
            and self.latest == other.latest
        )

    def __lt__(self, other: "MyTest") -> bool:
        """
        Compare two MyTest instances for sorting by id.
        """

        return self.id < other.id

    def __repr__(self) -> str:
        """
        String representation of MyTest instance.
        """

        return (
            f"<MyTest(id={self.id}, col_a={self.col_a},"
            f" col_b={self.col_b},"
            f" col_c={self.col_c}, latest={self.latest})>"
        )


@pytest.fixture(scope="function")
def db_engine() -> Generator[Engine, None, None]:
    """
    Pytest fixture for creating and tearing down the test database engine.

    :return: The SQLAlchemy engine instance.
    """

    engine = create_engine(TEST_DB_URL)
    MyTest.metadata.create_all(engine)
    yield engine
    MyTest.metadata.drop_all(engine)


@pytest.fixture(scope="function")
def db_session(db_engine: Engine) -> Generator[Session, None, None]:
    """
    Pytest fixture for creating and closing a database session.

    :param db_engine: The SQLAlchemy engine instance.
    :return: The SQLAlchemy session instance.
    """

    session = Session(db_engine)
    yield session
    session.close()
