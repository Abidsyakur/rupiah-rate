transform_dag = DAG(
    dag_id='transform_exchange_rates',
    description='Transform raw data dengan dbt',
    schedule_interval='0 3 * * *',      # 3 AM daily (after extract)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['transformation', 'dbt', 'etl'],
    depends_on_past=True
)

# Tasks:
# 1. dbt deps (install packages)
# 2. dbt run (staging models)
# 3. dbt run (intermediate models)
# 4. dbt run (mart models)
# 5. dbt test (data quality)
# 6. dbt docs generate (update documentation)
# 7. Send success alert