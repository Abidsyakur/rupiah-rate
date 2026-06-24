-- tests/singular/test_no_duplicate_rates.sql
SELECT
    from_currency_id,
    to_currency_id,
    rate_date,
    source_id,
    COUNT(*) as cnt
FROM {{ ref('stg_exchange_rates') }}
GROUP BY 1,2,3,4
HAVING COUNT(*) > 1