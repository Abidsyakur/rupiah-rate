-- fct_exchange_rates.sql
SELECT
    r.rate_id,
    fc.currency_key as from_currency_key,
    tc.currency_key as to_currency_key,
    r.rate,
    r.timestamp,
    CAST(r.timestamp AS DATE) as rate_date,
    s.source_key,
    r.data_quality_score,
    
    -- Calculations
    ROUND((r.rate - LAG(r.rate) OVER (PARTITION BY r.from_currency_id, r.to_currency_id ORDER BY r.timestamp)) / 
          LAG(r.rate) OVER (PARTITION BY r.from_currency_id, r.to_currency_id ORDER BY r.timestamp) * 100, 4) as pct_change,
    
    -- Quality flags
    CASE 
        WHEN r.data_quality_score < 0.7 THEN 'Low'
        WHEN r.data_quality_score < 0.9 THEN 'Medium'
        ELSE 'High'
    END as quality_level
    
FROM {{ ref('stg_exchange_rates') }} r
LEFT JOIN {{ ref('dim_currencies') }} fc ON r.from_currency_id = fc.currency_id
LEFT JOIN {{ ref('dim_currencies') }} tc ON r.to_currency_id = tc.currency_id
LEFT JOIN {{ ref('dim_api_sources') }} s ON r.source_id = s.source_id