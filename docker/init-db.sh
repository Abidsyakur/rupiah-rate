#!/usr/bin/env bash
# =============================================================================
# docker/init-db.sh
# =============================================================================
# Berjalan otomatis saat pertama kali container Postgres dinyalakan.
# Karena rupiah_db sudah dibuat otomatis oleh POSTGRES_DB di docker-compose,
# skrip ini bertugas membuat database tambahan bernama `airflow` untuk Airflow.
# =============================================================================
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE airflow;
EOSQL

echo "[init-db.sh] Database 'airflow' berhasil dibuat mendampingi 'rupiah_db'."