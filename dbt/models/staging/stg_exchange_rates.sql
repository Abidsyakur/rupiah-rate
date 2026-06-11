-- Staging: Clean and prepare raw exchange rate data

{{
    config(
        materialized='table',
        schema='staging',
        tags=['daily']
    )
}}

SELECT
    id,
    date,
    currency_pair,
    opening_rate,
    closing_rate,
    highest_rate,
    lowest_rate,
    volume,
    source,
    created_at,
    updated_at,
    ROW_NUMBER() OVER (PARTITION BY date, currency_pair, source ORDER BY updated_at DESC) as rn
FROM {{ source('raw', 'exchange_rates') }}
WHERE date IS NOT NULL
    AND closing_rate IS NOT NULL

QUALIFY rn = 1
