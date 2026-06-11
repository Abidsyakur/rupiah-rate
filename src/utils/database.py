"""
src/utils/database.py
=====================
Database utilities for exchange rate storage.
"""

from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manage database connections and sessions."""

    def __init__(self, connection_string: str):
        """
        Initialize database manager.

        Parameters
        ----------
        connection_string : str
            PostgreSQL connection string.
        """
        self.connection_string = connection_string
        self.engine = create_engine(
            connection_string,
            echo=False,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,  # Test connections before use
        )
        self.SessionLocal = sessionmaker(bind=self.engine)

        # Setup logging for SQL
        @event.listens_for(Engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            pass  # PostgreSQL doesn't need PRAGMA

    def get_session(self) -> Session:
        """Get a new database session."""
        return self.SessionLocal()

    def health_check(self) -> bool:
        """Check database connectivity."""
        try:
            with self.engine.connect() as conn:
                conn.execute("SELECT 1")
            logger.info("Database health check passed")
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

    def close(self):
        """Close all connections."""
        self.engine.dispose()
        logger.info("Database connections closed")
