/*
================================================================================
test_rate_consistency.sql
================================================================================
Type        : Singular test
Target      : {{ ref('fct_daily_snapshots') }}
Description :
    Asserts that OHLC values within each daily snapshot are internally
    logically consistent. FX (and any OHLC) data must always satisfy:

        rate_low  <= rate_open  <= rate_high
        rate_low  <= rate_close <= rate_high
        rate_low  <= rate_high

    A dbt test PASSES when this query returns ZERO rows. Any row returned
    represents a day where the open/high/low/close relationship is
    mathematically impossible given how OHLC is defined — high is by
    definition the maximum tick of the day, and low is the minimum, so
    every other price (open, close) must fall within [low, high].

    Also checks:
    - rate_open, rate_high, rate_low, rate_close are all strictly positive
      (mirrors the DB-level ck_daily_snapshots_rates_positive constraint,
      re-asserted here in case dbt builds this table from a source that
      bypassed the original constraint)
    - daily_change is consistent with rate_close - rate_open (catches
      calculation bugs introduced by future refactors of
      int_daily_exchange_rates.sql)

    Common root causes if this test fails:
    - A bug in the ROW_NUMBER-based open/close extraction logic in
      int_daily_exchange_rates.sql (e.g. incorrect ORDER BY direction)
    - Tied timestamps causing ambiguous "first" or "last" tick selection
    - A source data error where an out-of-range price was not caught by
      the validators.py RANGE_CHECK before loading
================================================================================
*/

with

snapshots as (

    select
        snapshot_id,
        rate_date,
        currency_pair,
        source_name,
        rate_open,
        rate_high,
        rate_low,
        rate_close,
        daily_change

    from {{ ref('fct_daily_snapshots') }}

),

-- -------------------------------------------------------------------------
-- Check 1: rate_high must be >= rate_low
-- -------------------------------------------------------------------------
high_below_low as (

    select
        snapshot_id,
        rate_date,
        currency_pair,
        source_name,
        'rate_high < rate_low'              as failure_reason,
        rate_high,
        rate_low,
        rate_open,
        rate_close

    from snapshots
    where rate_high < rate_low

),

-- -------------------------------------------------------------------------
-- Check 2: rate_open must fall within [rate_low, rate_high]
-- -------------------------------------------------------------------------
open_out_of_range as (

    select
        snapshot_id,
        rate_date,
        currency_pair,
        source_name,
        'rate_open outside [rate_low, rate_high]'   as failure_reason,
        rate_high,
        rate_low,
        rate_open,
        rate_close

    from snapshots
    where rate_open < rate_low or rate_open > rate_high

),

-- -------------------------------------------------------------------------
-- Check 3: rate_close must fall within [rate_low, rate_high]
-- -------------------------------------------------------------------------
close_out_of_range as (

    select
        snapshot_id,
        rate_date,
        currency_pair,
        source_name,
        'rate_close outside [rate_low, rate_high]'  as failure_reason,
        rate_high,
        rate_low,
        rate_open,
        rate_close

    from snapshots
    where rate_close < rate_low or rate_close > rate_high

),

-- -------------------------------------------------------------------------
-- Check 4: all OHLC values must be strictly positive
-- -------------------------------------------------------------------------
non_positive_rates as (

    select
        snapshot_id,
        rate_date,
        currency_pair,
        source_name,
        'one or more OHLC values <= 0'              as failure_reason,
        rate_high,
        rate_low,
        rate_open,
        rate_close

    from snapshots
    where rate_open <= 0
       or rate_high <= 0
       or rate_low  <= 0
       or rate_close <= 0

),

-- -------------------------------------------------------------------------
-- Check 5: daily_change must equal rate_close - rate_open (within a
-- small tolerance for floating-point / NUMERIC rounding artefacts)
-- -------------------------------------------------------------------------
inconsistent_daily_change as (

    select
        snapshot_id,
        rate_date,
        currency_pair,
        source_name,
        'daily_change does not match rate_close - rate_open'  as failure_reason,
        rate_high,
        rate_low,
        rate_open,
        rate_close

    from snapshots
    where abs(daily_change - (rate_close - rate_open)) > 0.000001

),

-- -------------------------------------------------------------------------
-- Combine all failure types into one result set
-- -------------------------------------------------------------------------
combined as (

    select * from high_below_low
    union all
    select * from open_out_of_range
    union all
    select * from close_out_of_range
    union all
    select * from non_positive_rates
    union all
    select * from inconsistent_daily_change

)

select * from combined