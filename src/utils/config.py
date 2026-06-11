"""Configuration management for different environments"""

import os
from typing import Dict, Any


class Config:
    """Base configuration class"""

    DEBUG = False
    TESTING = False

    # Database
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))
    DB_NAME = os.getenv("DB_NAME", "rupiah_rates")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")

    # API
    API_KEY = os.getenv("API_KEY", "")
    API_TIMEOUT = int(os.getenv("API_TIMEOUT", "30"))


class DevelopmentConfig(Config):
    """Development configuration"""
    DEBUG = True


class StagingConfig(Config):
    """Staging configuration"""
    pass


class ProductionConfig(Config):
    """Production configuration"""
    TESTING = False


def get_config(env: str = None) -> Config:
    """Get configuration based on environment"""
    env = env or os.getenv("ENVIRONMENT", "development")

    config_map = {
        "development": DevelopmentConfig,
        "staging": StagingConfig,
        "production": ProductionConfig,
    }

    return config_map.get(env, DevelopmentConfig)()
