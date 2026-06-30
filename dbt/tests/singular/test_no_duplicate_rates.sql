/*
================================================================================
test_no_duplicate_rates.sql
================================================================================
Type        : Singular test
Target      : {{ ref('fct_daily_snapshots') }}
             {{ ref('fct_exchange_rates') }}
Description :
    Asserts there are no duplicate rows at the business grain for both the
    daily snapshot fact table and the tick-level fact table.

    A dbt test PASSES when this query returns ZERO rows. Any row returned
    here represents a duplicate that violates the expected grain and must
    be investigated before the data is trusted downstream.

    Why this exists in addition to unique_checks.yml:
    unique_checks.yml uses dbt_utils.unique_combination_of_columns, which
    is a generic test bound to a single model. This singular test instead
    checks BOTH fact tables in one place and surfaces the actual duplicate
    rows (with their counts) for easier debugging — generic test failures
    only tell you "N rows failed," not which keys collided.

    Common root causes if this test fails:
    - A loader bug that bypassed the upsert idempotency key
      (from_currency_id, to_currency_id, timestamp, source_id)
    - A dbt incremental model misconfiguration causing reprocessing
    - A source data quality issue (same observation reported twice by
      the upstream API with slightly different metadata)
================================================================================
*/

with

-- -------------------------------------------------------------------------
-- Check 1: fct_daily_snapshots should have exactly one row per
-- (currency_pair, source_id, rate_date)
-- -------------------------------------------------------------------------
duplicate_daily_snapshots as (

    select
        currency_pair,
        source_id,
        rate_date,
        count(*)                as duplicate_count

    from {{ ref('fct_daily_snapshots') }}
    group by currency_pair, source_id, rate_date
    having count(*) > 1

),

-- -------------------------------------------------------------------------
-- Check 2: fct_exchange_rates should have exactly one row per
-- (from_currency_id, to_currency_id, source_id, rate_timestamp)
-- -------------------------------------------------------------------------
duplicate_tick_rates as (

    select
        from_currency_id,
        to_currency_id,
        source_id,
        rate_timestamp,
        count(*)                as duplicate_count

    from {{ ref('fct_exchange_rates') }}
    group by from_currency_id, to_currency_id, source_id, rate_timestamp
    having count(*) > 1

),

-- -------------------------------------------------------------------------
-- Combine both checks into one result set, labelled by source table,
-- so a single failing test clearly shows which table(s) have duplicates.
-- -------------------------------------------------------------------------
combined as (

    select
        'fct_daily_snapshots'           as failing_model,
        currency_pair::text             as grain_key_1,
        source_id::text                 as grain_key_2,
        rate_date::text                 as grain_key_3,
        duplicate_count
    from duplicate_daily_snapshots

    union all

    select
        'fct_exchange_rates'            as failing_model,
        from_currency_id::text || '_' || to_currency_id::text
                                        as grain_key_1,
        source_id::text                 as grain_key_2,
        rate_timestamp::text            as grain_key_3,
        duplicate_count
    from duplicate_tick_rates

)

select * from combined