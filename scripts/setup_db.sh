#!/bin/bash
# Database setup script

set -e

echo "Setting up Rupiah Rate database..."

# Create database
psql -U postgres -c "CREATE DATABASE rupiah_rates;"

# Create user
psql -U postgres -c "CREATE USER app_user WITH PASSWORD 'secure_password';"

# Grant privileges
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE rupiah_rates TO app_user;"

# Connect and setup schema
psql -U postgres rupiah_rates << EOF
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS raw.exchange_rates (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    currency_pair VARCHAR(10) NOT NULL,
    opening_rate DECIMAL(10, 4),
    closing_rate DECIMAL(10, 4),
    highest_rate DECIMAL(10, 4),
    lowest_rate DECIMAL(10, 4),
    volume BIGINT,
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, currency_pair, source)
);

CREATE INDEX IF NOT EXISTS idx_exchange_rates_date ON raw.exchange_rates(date);
CREATE INDEX IF NOT EXISTS idx_exchange_rates_pair ON raw.exchange_rates(currency_pair);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO app_user;
EOF

echo "Database setup completed!"
