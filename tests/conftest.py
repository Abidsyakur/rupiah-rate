"""
tests/conftest.py
==================
Shared pytest configuration and fixtures for the
rupiah-exchange-rate-intelligence test suite.

Responsibilities
----------------
- Add ``src/`` to ``sys.path`` once, for every test module
  (so ``from utils.database import ...`` and
  ``from etl.extractors import ...`` work without per-file hacks).
- Provide a session-scoped in-memory SQLite engine + schema.
- Provide a function-scoped, transaction-isolated SQLAlchemy session
  (SQLAlchemy 2.0 style) for any test that needs DB access.

Usage
-----
Any test file under ``tests/`` can simply request the ``session`` fixture:

    def test_something(session):
        session.add(Currency(code="USD", name="US Dollar"))
        session.flush()
"""

from __future__ import annotations

import pathlib
import sys

import pytest
from sqlalchemy.orm import Session

# ---------------------------------------------------------------------------
# Make src/ importable for every test module (etl.*, models.*, etc.)
# ---------------------------------------------------------------------------
SRC_PATH = pathlib.Path(__file__).parent.parent / "src"
sys.path.insert(0, str(SRC_PATH))

from src.utils.database import Base, get_engine  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SQLITE_URL = "sqlite://"  # pure in-memory, no file


# ---------------------------------------------------------------------------
# Engine fixture — created once per test session
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def engine():
    """
    Session-scoped in-memory SQLite engine with the full schema applied.

    Using ``scope="session"`` avoids recreating ~6 tables + indexes for
    every single test; isolation between tests is instead provided by the
    function-scoped ``session`` fixture below (transaction rollback).
    """
    eng = get_engine(database_url=SQLITE_URL, env="dev")
    Base.metadata.create_all(eng)
    yield eng
    Base.metadata.drop_all(eng)
    eng.dispose()


# ---------------------------------------------------------------------------
# Session fixture — fresh transaction per test (SQLAlchemy 2.0 style)
# ---------------------------------------------------------------------------

@pytest.fixture
def session(engine) -> Session:
    """
    Function-scoped database session, isolated via a rolled-back transaction.

    SQLAlchemy 2.0 pattern
    ----------------------
    - ``engine.connect()`` returns a ``Connection``.
    - ``conn.begin()`` starts an explicit outer transaction.
    - ``Session(conn)`` binds the ORM session to that connection so all
      ORM operations participate in the same transaction.
    - On teardown, the outer transaction is rolled back, discarding every
      change made during the test — the schema itself is never recreated.
    """
    conn = engine.connect()
    outer_tx = conn.begin()

    sess = Session(bind=conn)

    yield sess

    sess.close()
    if outer_tx.is_active:
        outer_tx.rollback()
    conn.close()