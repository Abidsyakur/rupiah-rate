load_dag = DAG(
    dag_id='load_analytics',
    description='Load analytics data ke dashboard databases',
    schedule_interval='0 4 * * *',      # 4 AM daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['loading', 'analytics']
)

# Tasks:
# 1. Export mart tables
# 2. Load ke data warehouse
# 3. Refresh dashboard cache
# 4. Send success alert