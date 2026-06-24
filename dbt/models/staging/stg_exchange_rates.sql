-- stg_exchange_rates.sql
SELECT
    rate_id,
    from_currency_id,
    to_currency_id,
    rate,
    CAST(timestamp AS DATE) as rate_date,
    EXTRACT(HOUR FROM timestamp) as rate_hour,
    source_id,
    data_quality_score,
    is_valid,
    created_at,
    updated_at
FROM {{ source('raw', 'exchange_rates') }}
WHERE is_valid = TRUE
    AND data_quality_score >= 0.7