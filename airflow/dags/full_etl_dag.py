full_etl_dag = DAG(
    dag_id='full_etl_pipeline',
    description='Complete ETL pipeline: Extract → Transform → Load',
    schedule_interval='0 2 * * *',      # 2 AM daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['etl', 'orchestration', 'critical'],
    sla_miss_callback=alert_sla_miss,
    on_failure_callback=alert_failure
)

# Task flow:
# 1. Extract → Transform → Load (sequential)
# 2. Error handling & retries
# 3. Success notifications
# 4. Failure alerts