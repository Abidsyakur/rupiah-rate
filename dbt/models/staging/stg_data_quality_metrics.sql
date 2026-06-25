/*
================================================================================
stg_data_quality_metrics.sql
================================================================================
Layer       : Staging
Source      : {{ source('raw', 'data_quality_metrics') }}
Materialized: view
Description :
    Cleans and enriches per-record data quality check results.

    The data_quality_metrics table stores one row per (rate_id, check_name)
    pair, recording whether a specific named check passed or failed for a
    given exchange rate observation. This staging model:

    - Standardises check_name to uppercase
    - Casts anomaly_score to NUMERIC for safe arithmetic
    - Derives check_category from check_name
    - Derives severity_level for failed checks
    - Adds is_anomaly boolean flag
    - Adds anomaly_band classification (NONE / LOW / MEDIUM / HIGH / CRITICAL)
    - Preserves created_at for freshness checks

Downstream  : int_quality_summary.sql, int_exchange_rates_enriched.sql
Tests       : unique(metric_id), not_null(metric_id, rate_id, check_name,
              check_passed), accepted_values(check_name)
================================================================================
*/

with

source as (

    select
        metric_id,
        rate_id,
        check_name,
        check_passed,
        anomaly_score,
        created_at

    from {{ source('raw', 'data_quality_metrics') }}

),

cleaned as (

    select

        -- ----------------------------------------------------------------
        -- Primary key
        -- ----------------------------------------------------------------
        metric_id,

        -- ----------------------------------------------------------------
        -- Foreign key
        -- ----------------------------------------------------------------
        rate_id,

        -- ----------------------------------------------------------------
        -- Check identification — standardised
        -- ----------------------------------------------------------------

        -- Uppercase for consistent filtering downstream
        upper(trim(check_name))                             as check_name,

        -- Original display name (kept for reports)
        trim(check_name)                                    as check_name_display,

        -- Boolean result of the check
        check_passed,

        -- ----------------------------------------------------------------
        -- Anomaly score
        -- ----------------------------------------------------------------

        -- Cast to NUMERIC(3,2) for safe aggregation; NULL = not an anomaly check
        anomaly_score::numeric(3, 2)                        as anomaly_score,

        -- ----------------------------------------------------------------
        -- Derived: check category
        -- ----------------------------------------------------------------

        /*
        Groups checks into three categories to help downstream models
        summarise quality across different dimensions:

          data_completeness  — checks that look for missing / null values
                               (NULL_CHECK)
          data_validity      — checks that validate value ranges and formats
                               (RANGE_CHECK)
          statistical        — checks that detect statistical anomalies
                               (ANOMALY_CHECK)
          unknown            — any check name not in the known list
        */
        case upper(trim(check_name))
            when 'NULL_CHECK'    then 'data_completeness'
            when 'RANGE_CHECK'   then 'data_validity'
            when 'ANOMALY_CHECK' then 'statistical'
            else                      'unknown'
        end                                                 as check_category,

        -- ----------------------------------------------------------------
        -- Derived: severity level for FAILED checks
        -- ----------------------------------------------------------------

        /*
        When a check fails, classify the severity so downstream alerting
        models can prioritise which failures to escalate:

          CRITICAL  — NULL_CHECK failure: missing data is a hard blocker
          HIGH      — RANGE_CHECK failure: value outside expected bounds
          MEDIUM    — ANOMALY_CHECK failure: statistical outlier detected
          NONE      — check passed (no severity)
          UNKNOWN   — unrecognised check type

        Note: severity is NULL-safe — if check_passed = true the level
        is always 'NONE' regardless of check type.
        */
        case
            when check_passed = true then 'NONE'
            when upper(trim(check_name)) = 'NULL_CHECK'    then 'CRITICAL'
            when upper(trim(check_name)) = 'RANGE_CHECK'   then 'HIGH'
            when upper(trim(check_name)) = 'ANOMALY_CHECK' then 'MEDIUM'
            else                                                'UNKNOWN'
        end                                                 as severity_level,

        -- ----------------------------------------------------------------
        -- Derived: anomaly flags (only meaningful for ANOMALY_CHECK rows)
        -- ----------------------------------------------------------------

        /*
        Simple boolean: TRUE when this is an anomaly check that failed.
        More specific than check_passed because anomaly_score provides
        additional gradient information not captured in the boolean.
        */
        case
            when upper(trim(check_name)) = 'ANOMALY_CHECK'
                 and check_passed = false
            then true
            else false
        end                                                 as is_anomaly,

        /*
        Anomaly severity band derived from anomaly_score (0.00 – 1.00):
          NONE      — check passed or not an anomaly check (anomaly_score NULL)
          LOW       — anomaly_score < 0.4   (mild outlier)
          MEDIUM    — anomaly_score 0.4 – 0.69
          HIGH      — anomaly_score 0.7 – 0.89
          CRITICAL  — anomaly_score >= 0.9  (extreme outlier)

        Thresholds align with the pipeline's AnomalyLevelEnum
        (NORMAL / WARNING / CRITICAL) mapped to a finer-grained scale.
        */
        case
            when check_passed = true or anomaly_score is null then 'NONE'
            when anomaly_score < 0.4                          then 'LOW'
            when anomaly_score < 0.7                          then 'MEDIUM'
            when anomaly_score < 0.9                          then 'HIGH'
            else                                                   'CRITICAL'
        end                                                 as anomaly_band,

        -- ----------------------------------------------------------------
        -- Audit timestamp
        -- ----------------------------------------------------------------
        created_at::timestamptz                             as created_at

    from source

)

/*
No row filtering on quality metrics — keeping all rows (including
passed checks) gives downstream models the flexibility to compute
pass rates, failure rates, and trend analysis. Filtering by
check_passed or severity_level is left to intermediate/mart models.
*/
select * from cleaned