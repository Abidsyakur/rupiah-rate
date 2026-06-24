-- int_daily_exchange_rates.sql
SELECT
    rate_date,
    from_currency_id,
    to_currency_id,
    source_id,
    
    -- OHLC calculations
    MIN(rate) as rate_low,
    MAX(rate) as rate_high,
    FIRST_VALUE(rate) OVER (PARTITION BY rate_date, from_currency_id, to_currency_id ORDER BY rate_hour) as rate_open,
    LAST_VALUE(rate) OVER (PARTITION BY rate_date, from_currency_id, to_currency_id ORDER BY rate_hour) as rate_close,
    AVG(rate) as rate_avg,
    
    -- Technical indicators
    AVG(rate) OVER (ORDER BY rate_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as rate_ma7,
    
    -- Change calculations
    LAG(rate) OVER (ORDER BY rate_date) as rate_previous_day,
    
    -- Count metrics
    COUNT(*) as records_count,
    COUNT(DISTINCT source_id) as sources_count
    
FROM {{ ref('stg_exchange_rates') }}
GROUP BY rate_date, from_currency_id, to_currency_id, source_id