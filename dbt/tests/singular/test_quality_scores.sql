/*
================================================================================
test_quality_scores.sql
================================================================================
Type        : Singular test
Target      : {{ ref('stg_exchange_rates') }}
             {{ ref('stg_data_quality_metrics') }}
             {{ ref('int_quality_summary') }}
             {{ ref('fct_daily_snapshots') }}
Description :
    Asserts that every quality / anomaly score in the pipeline stays within
    its defined [0.00, 1.00] bound, and that aggregate quality metrics are
    internally consistent with their component parts.

    A dbt test PASSES when this query returns ZERO rows. Any row returned
    represents a quality score that violated its contract — either out of
    bounds, or a derived aggregate that doesn't logically follow from its
    inputs.

    Checks performed:
    ───────────────────
    1. stg_exchange_rates.data_quality_score must be in [0.00, 1.00]
       (mirrors the DB-level ck_exchange_rates_quality_score_range
       constraint — re-asserted here in case a future model bypasses it)

    2. stg_data_quality_metrics.anomaly_score must be in [0.00, 1.00]
       when present (mirrors ck_data_quality_anomaly_score_range)

    3. int_quality_summary.daily_pass_rate_pct must be in [0, 100]
       (it's a percentage; anything outside this range indicates a
       division or aggregation bug)

    4. int_quality_summary.avg_composite_score must be in [0.00, 1.00]
       (composite_score is itself bounded to [0,1] in rate_quality, so
       its average must also stay within that range)

    5. fct_daily_snapshots.daily_pass_rate_pct must be in [0, 100]
       (re-asserted at the marts layer in case a join introduces drift)

    6. int_quality_summary: rates_all_passed must never exceed
       total_rates_evaluated (a basic sanity bound — you cannot have more
       passing rates than rates evaluated)

    7. int_quality_summary: rates_with_anomaly must never exceed
       total_rates_evaluated

    Common root causes if this test fails:
    - A bug in validators.py's calculate_quality_score() producing values
      outside [0,1] (e.g. weights not summing to 1.0)
    - A dbt aggregation bug (e.g. SUM used where AVG was intended)
    - A loader bug that wrote an out-of-range score directly to the DB,
      bypassing both the Python validator and the DB CHECK constraint
================================================================================
*/

with

-- -------------------------------------------------------------------------
-- Check 1: stg_exchange_rates.data_quality_score out of [0,1] bounds
-- -------------------------------------------------------------------------
rate_quality_out_of_bounds as (

    select
        rate_id::text                       as record_id,
        'stg_exchange_rates'                 as failing_model,
        'data_quality_score out of [0,1] bounds'  as failure_reason,
        data_quality_score::text             as failing_value

    from {{ ref('stg_exchange_rates') }}
    where data_quality_score is not null
      and (data_quality_score < 0.0 or data_quality_score > 1.0)

),

-- -------------------------------------------------------------------------
-- Check 2: stg_data_quality_metrics.anomaly_score out of [0,1] bounds
-- -------------------------------------------------------------------------
anomaly_score_out_of_bounds as (

    select
        metric_id::text                      as record_id,
        'stg_data_quality_metrics'            as failing_model,
        'anomaly_score out of [0,1] bounds'   as failure_reason,
        anomaly_score::text                  as failing_value

    from {{ ref('stg_data_quality_metrics') }}
    where anomaly_score is not null
      and (anomaly_score < 0.0 or anomaly_score > 1.0)

),

-- -------------------------------------------------------------------------
-- Check 3: int_quality_summary.daily_pass_rate_pct out of [0,100] bounds
-- -------------------------------------------------------------------------
daily_pass_rate_out_of_bounds as (

    select
        quality_summary_id::text             as record_id,
        'int_quality_summary'                as failing_model,
        'daily_pass_rate_pct out of [0,100] bounds'  as failure_reason,
        daily_pass_rate_pct::text            as failing_value

    from {{ ref('int_quality_summary') }}
    where daily_pass_rate_pct is not null
      and (daily_pass_rate_pct < 0 or daily_pass_rate_pct > 100)

),

-- -------------------------------------------------------------------------
-- Check 4: int_quality_summary.avg_composite_score out of [0,1] bounds
-- -------------------------------------------------------------------------
composite_score_out_of_bounds as (

    select
        quality_summary_id::text             as record_id,
        'int_quality_summary'                as failing_model,
        'avg_composite_score out of [0,1] bounds'  as failure_reason,
        avg_composite_score::text            as failing_value

    from {{ ref('int_quality_summary') }}
    where avg_composite_score is not null
      and (avg_composite_score < 0.0 or avg_composite_score > 1.0)

),

-- -------------------------------------------------------------------------
-- Check 5: fct_daily_snapshots.daily_pass_rate_pct out of [0,100] bounds
-- (re-asserted at the marts layer)
-- -------------------------------------------------------------------------
mart_pass_rate_out_of_bounds as (

    select
        snapshot_id::text                    as record_id,
        'fct_daily_snapshots'                 as failing_model,
        'daily_pass_rate_pct out of [0,100] bounds'  as failure_reason,
        daily_pass_rate_pct::text            as failing_value

    from {{ ref('fct_daily_snapshots') }}
    where daily_pass_rate_pct is not null
      and (daily_pass_rate_pct < 0 or daily_pass_rate_pct > 100)

),

-- -------------------------------------------------------------------------
-- Check 6: rates_all_passed must never exceed total_rates_evaluated
-- -------------------------------------------------------------------------
passed_exceeds_evaluated as (

    select
        quality_summary_id::text             as record_id,
        'int_quality_summary'                as failing_model,
        'rates_all_passed exceeds total_rates_evaluated'  as failure_reason,
        (rates_all_passed::text || ' > ' || total_rates_evaluated::text)
                                              as failing_value

    from {{ ref('int_quality_summary') }}
    where rates_all_passed > total_rates_evaluated

),

-- -------------------------------------------------------------------------
-- Check 7: rates_with_anomaly must never exceed total_rates_evaluated
-- -------------------------------------------------------------------------
anomalies_exceed_evaluated as (

    select
        quality_summary_id::text             as record_id,
        'int_quality_summary'                as failing_model,
        'rates_with_anomaly exceeds total_rates_evaluated'  as failure_reason,
        (rates_with_anomaly::text || ' > ' || total_rates_evaluated::text)
                                              as failing_value

    from {{ ref('int_quality_summary') }}
    where rates_with_anomaly > total_rates_evaluated

),

-- -------------------------------------------------------------------------
-- Combine every failure type into one result set
-- -------------------------------------------------------------------------
combined as (

    select * from rate_quality_out_of_bounds
    union all
    select * from anomaly_score_out_of_bounds
    union all
    select * from daily_pass_rate_out_of_bounds
    union all
    select * from composite_score_out_of_bounds
    union all
    select * from mart_pass_rate_out_of_bounds
    union all
    select * from passed_exceeds_evaluated
    union all
    select * from anomalies_exceed_evaluated

)

select * from combined