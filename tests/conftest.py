"""Pytest configuration and fixtures"""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture(scope="session")
def db_engine():
    """Create database engine for tests"""
    engine = create_engine("postgresql://postgres:postgres@localhost:5432/rupiah_rates_test")
    return engine


@pytest.fixture(scope="function")
def db_session(db_engine):
    """Create database session for each test"""
    Session = sessionmaker(bind=db_engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def sample_exchange_rates():
    """Sample exchange rate data"""
    return [
        {
            "date": "2024-01-01",
            "currency_pair": "IDR/USD",
            "opening_rate": 15500.0,
            "closing_rate": 15520.0,
            "highest_rate": 15530.0,
            "lowest_rate": 15490.0,
            "volume": 1000000000,
            "source": "BI",
        },
        {
            "date": "2024-01-02",
            "currency_pair": "IDR/USD",
            "opening_rate": 15520.0,
            "closing_rate": 15540.0,
            "highest_rate": 15550.0,
            "lowest_rate": 15510.0,
            "volume": 1100000000,
            "source": "BI",
        },
    ]
