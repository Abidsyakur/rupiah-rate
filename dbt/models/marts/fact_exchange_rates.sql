-- Mart: Final analytics-ready fact table

{{
    config(
        materialized='table',
        schema='marts'
    )
}}

SELECT
    *,
    AVG(closing_rate) OVER (PARTITION BY currency_pair ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as ma_7d,
    AVG(closing_rate) OVER (PARTITION BY currency_pair ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as ma_30d,
    STDDEV(closing_rate) OVER (PARTITION BY currency_pair ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as volatility_30d
FROM {{ ref('int_exchange_rates_with_metrics') }}
