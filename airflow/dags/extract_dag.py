extract_dag = DAG(
    dag_id='extract_exchange_rates',
    description='Extract exchange rates dari yfinance & FRED API',
    schedule_interval='0 2 * * *',      # 2 AM daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['extraction', 'etl']
)

# Tasks:
# 1. Check APIs availability
# 2. Extract dari yfinance
# 3. Extract dari FRED
# 4. Merge results
# 5. Validate counts
# 6. Send success alert