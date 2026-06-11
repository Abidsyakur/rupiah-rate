-- Intermediate: Add calculated metrics

{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

SELECT
    *,
    ROUND(100.0 * (closing_rate - opening_rate) / opening_rate, 2) as daily_change_pct,
    highest_rate - lowest_rate as daily_range,
    ROW_NUMBER() OVER (PARTITION BY currency_pair ORDER BY date) as day_number
FROM {{ ref('stg_exchange_rates') }}
