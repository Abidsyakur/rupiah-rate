#!/bin/bash
# Deployment script

set -e

echo "Deploying Rupiah Rate Intelligence System..."

# Build
echo "Building application..."
python -m pip install -r requirements.txt

# Test
echo "Running tests..."
pytest tests/ -v

# Migrate database
echo "Running migrations..."
alembic upgrade head

# Deploy
echo "Deploying to production..."
docker build -t rupiah-rate:latest .
docker tag rupiah-rate:latest rupiah-rate:$(date +%Y%m%d)

echo "Deployment completed!"
