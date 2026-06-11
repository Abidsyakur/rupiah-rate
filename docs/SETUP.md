# Setup Guide

## Prerequisites

- Python 3.10 or higher
- PostgreSQL 13+
- Docker & Docker Compose (optional)
- Git

## Step 1: Clone Repository

```bash
git clone <repository-url>
cd rupiah-exchange-rate-intelligence
```

## Step 2: Create Virtual Environment

```bash
python -m venv venv

# Activate (Linux/Mac)
source venv/bin/activate

# Activate (Windows)
venv\Scripts\activate
```

## Step 3: Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

## Step 4: Configure Environment

```bash
cp config/dev.yaml .env
# Edit .env with your configuration
```

## Step 5: Initialize Database

```bash
bash scripts/setup_db.sh
```

## Step 6: Run Tests

```bash
pytest tests/ -v
```

## Step 7: Start Services

### Option A: Local Development

```bash
python -m src.main
```

### Option B: With Docker

```bash
docker-compose up -d
```

## Troubleshooting

### Database Connection Error

- Check PostgreSQL is running
- Verify connection string in .env
- Check credentials are correct

### Import Errors

- Ensure virtual environment is activated
- Reinstall requirements: `pip install -r requirements.txt`

### Permission Denied

- Make scripts executable: `chmod +x scripts/*.sh`

## Additional Setup

### dbt Configuration

```bash
cd dbt
dbt debug
dbt seed
dbt run
```

### Airflow Setup

```bash
airflow db init
airflow users create --username admin --password admin \
  --firstname Admin --lastname User --role Admin --email admin@example.com
airflow webserver -p 8080
```

## Next Steps

- Read [ARCHITECTURE.md](ARCHITECTURE.md) for system design
- Review [API_SPECIFICATIONS.md](API_SPECIFICATIONS.md) for data contracts
- Check [OPERATIONS.md](OPERATIONS.md) for operational guidelines
