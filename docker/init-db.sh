#!/usr/bin/env bash
# =============================================================================
# docker/init-db.sh
# =============================================================================
# Berjalan otomatis saat pertama kali container Postgres dinyalakan 
# (dimuat di dalam folder /docker-entrypoint-initdb.d/).
#
# Karena rupiah_db sudah dibuat otomatis oleh POSTGRES_DB di docker-compose,
# skrip ini memastikan database metadata tambahan bernama `airflow` dibuat 
# dengan hak akses penuh untuk user utama.
# =============================================================================
set -euo pipefail

# Menjalankan perintah SQL dengan pengamanan tanda petik ganda pada nama user
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE airflow OWNER "${POSTGRES_USER}";
    GRANT ALL PRIVILEGES ON DATABASE airflow TO "${POSTGRES_USER}";
EOSQL

echo "[init-db.sh] Database 'airflow' berhasil dibuat mendampingi '${POSTGRES_DB}' (Pemilik: ${POSTGRES_USER})."