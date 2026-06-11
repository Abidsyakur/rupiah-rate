# Operations Runbook

## Daily Operations

### Morning Checks

1. Verify Airflow DAG runs completed successfully
```bash
airflow dags list-runs --dag-id rupiah_pipeline_dag
```

2. Check data ingestion volume
```sql
SELECT COUNT(*) as record_count FROM raw_exchange_rates 
WHERE date = CURRENT_DATE;
```

3. Review quality metrics
```sql
SELECT * FROM data_quality_checks 
WHERE check_date = CURRENT_DATE 
AND status != 'PASSED';
```

### Handling Failures

#### Airflow Task Failure

1. Check task logs
```bash
airflow tasks log rupiah_pipeline_dag task_name 2024-01-15
```

2. Review error details and retry if needed
```bash
airflow tasks clear rupiah_pipeline_dag --task-id task_name -d
```

3. Monitor rerun completion

#### Data Quality Issues

1. Identify affected records
2. Review validation rules in `src/etl/validators.py`
3. Determine root cause (API issue, schema change, etc.)
4. Apply fix and backfill if needed

## Monitoring

### Key Metrics to Track

- Data ingestion latency
- Data quality pass rate
- Pipeline execution time
- Forecast accuracy (MAPE, RMSE)

### Alerting Rules

- Task failure: Alert immediately
- Data quality < 95%: Alert within 1 hour
- Pipeline latency > 30 min: Alert within 1 hour

## Backups

### Database Backups

```bash
# Full backup
pg_dump rupiah_rates > backup_$(date +%Y%m%d).sql

# Automated daily backup (cron)
0 2 * * * pg_dump rupiah_rates > /backups/backup_$(date +\%Y\%m\%d).sql
```

### Recovery Procedure

```bash
# Restore from backup
psql rupiah_rates < backup_20240115.sql
```

## Scaling Considerations

### Current Capacity
- ~1M records/day
- 3-year retention
- ~1GB/month growth

### When to Scale

- CPU usage consistently > 70%
- Query execution time > 5 seconds
- Storage > 80% capacity

### Scaling Options

1. **Vertical**: Increase server resources
2. **Horizontal**: Partition tables by date
3. **Archive**: Move old data to cold storage

## Emergency Procedures

### Complete Pipeline Reset

```bash
# 1. Stop Airflow
airflow webserver --stop

# 2. Clear database (CAUTION!)
psql -U postgres -c "DROP DATABASE rupiah_rates;"

# 3. Reinitialize
bash scripts/setup_db.sh

# 4. Restart services
docker-compose up -d
```

## Contact & Escalation

- **Data Team Lead**: data-lead@company.com
- **DevOps**: devops@company.com
- **On-call**: See Slack channel #data-oncall
