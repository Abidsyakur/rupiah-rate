/*
================================================================================
dim_quality_metrics.sql
================================================================================
Layer       : Marts (Dimension)
Upstream    : {{ ref('stg_data_quality_metrics') }}
Materialized: table
Description :
    Reference dimension describing each distinct quality check type used
    across the pipeline, enriched with aggregate pass/fail statistics so
    dashboards can show "what does this check mean and how often does it
    fail" without re-aggregating the full metrics fact table each time.

Grain       : One row per check_name
Used by     : fct_exchange_rates (quality flag join), quality dashboards
================================================================================
*/

with

metrics as (

    select
        check_name,
        check_name_display,
        check_category,
        check_passed,
        severity_level,
        is_anomaly,
        anomaly_band

    from {{ ref('stg_data_quality_metrics') }}

),

-- -------------------------------------------------------------------------
-- Aggregate pass/fail statistics per check type (all-time)
-- -------------------------------------------------------------------------
check_stats as (

    select

        check_name,
        max(check_name_display)                            as check_name_display,
        max(check_category)                                as check_category,

        count(*)                                            as total_evaluations,
        count(*) filter (where check_passed = true)         as total_passed,
        count(*) filter (where check_passed = false)        as total_failed,

        round(
            count(*) filter (where check_passed = true)::numeric
            / nullif(count(*), 0) * 100,
            2
        )                                                   as overall_pass_rate_pct,

        -- Anomaly-specific stats (NULL for non-anomaly check types)
        count(*) filter (where is_anomaly = true)           as total_anomalies_flagged,

        mode() within group (
            order by severity_level
        ) filter (where check_passed = false)               as typical_failure_severity

    from metrics
    group by check_name

),

final as (

    select

        -- ----------------------------------------------------------------
        -- Primary key (natural key — check_name is a fixed enum)
        -- ----------------------------------------------------------------
        check_name,
        check_name_display,
        check_category,

        -- ----------------------------------------------------------------
        -- Static business description
        -- ----------------------------------------------------------------

        /*
        Human-readable explanation of what each check validates.
        Sourced from src/etl/validators.py docstrings — kept in sync
        manually since check definitions change infrequently.
        */
        case check_name
            when 'NULL_CHECK'
                then 'Verifies the rate value is not null/missing. A failure here means the observation has no usable rate value.'
            when 'RANGE_CHECK'
                then 'Verifies the rate falls within an expected min/max bound for the currency pair. A failure indicates a likely data error or extreme market event.'
            when 'ANOMALY_CHECK'
                then 'Statistical outlier detection (Z-score based) comparing the rate to recent historical values. A failure flags a value that deviates significantly from the recent trend.'
            else 'Unrecognised check type — see src/etl/validators.py for current definitions.'
        end                                                 as check_description,

        /*
        Default severity if this check fails, matching the weighting used
        in int_quality_summary's composite_score calculation.
        */
        case check_name
            when 'NULL_CHECK'    then 'CRITICAL'
            when 'RANGE_CHECK'   then 'HIGH'
            when 'ANOMALY_CHECK' then 'MEDIUM'
            else                      'UNKNOWN'
        end                                                 as default_severity,

        -- Weight used in the composite_score formula (int_quality_summary)
        case check_name
            when 'NULL_CHECK'    then 0.4
            when 'RANGE_CHECK'   then 0.4
            when 'ANOMALY_CHECK' then 0.2
            else                      0.0
        end                                                 as composite_score_weight,

        -- ----------------------------------------------------------------
        -- Aggregate statistics (all-time, refreshed each dbt run)
        -- ----------------------------------------------------------------
        s.total_evaluations,
        s.total_passed,
        s.total_failed,
        s.overall_pass_rate_pct,
        s.total_anomalies_flagged,
        s.typical_failure_severity,

        -- ----------------------------------------------------------------
        -- Health indicator for this check type
        -- ----------------------------------------------------------------
        case
            when s.overall_pass_rate_pct is null            then 'NO_DATA'
            when s.overall_pass_rate_pct >= 95               then 'HEALTHY'
            when s.overall_pass_rate_pct >= 80               then 'WATCH'
            else                                                  'CONCERNING'
        end                                                 as check_health_status,

        -- ----------------------------------------------------------------
        -- Audit
        -- ----------------------------------------------------------------
        current_timestamp                                   as dbt_updated_at

    from check_stats s

)

select * from final