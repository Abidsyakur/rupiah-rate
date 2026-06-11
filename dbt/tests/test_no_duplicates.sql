-- Test for duplicate records

SELECT
    date,
    currency_pair,
    source,
    COUNT(*) as cnt
FROM {{ ref('stg_exchange_rates') }}
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
