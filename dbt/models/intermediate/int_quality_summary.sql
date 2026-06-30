/*
================================================================================
int_quality_summary.sql
================================================================================
Layer       : Intermediate
Upstream    : {{ ref('stg_data_quality_metrics') }}
             {{ ref('stg_exchange_rates') }}
             {{ ref('stg_currencies') }}      (from + to)
             {{ ref('stg_api_sources') }}
Materialized: table
Description :
    Joins per-record quality check results with exchange rate observations to
    produce two aggregation levels in one model via CTEs:

    Level 1 — rate_id grain (one row per exchange rate observation):
      Pivots the three named checks (NULL_CHECK, RANGE_CHECK, ANOMALY_CHECK)
      into columns, computes composite quality score and overall pass/fail.

    Level 2 — pair + source + date grain (one row per currency pair per day):
      Rolls up Level 1 into daily quality KPIs suitable for trend monitoring
      and alerting dashboards.

    Metrics at rate_id grain
    ─────────────────────────
      null_check_passed     Result of NULL_CHECK for this rate
      range_check_passed    Result of RANGE_CHECK for this rate
      anomaly_check_passed  Result of ANOMALY_CHECK for this rate
      checks_run            Number of distinct checks recorded for this rate
      checks_passed         Number of checks that passed
      checks_failed         Number of checks that failed
      all_checks_passed     TRUE only when all three checks passed
      composite_score       Weighted score:
                              NULL_CHECK  → weight 0.4  (critical)
                              RANGE_CHECK → weight 0.4  (critical)
                              ANOMALY     → weight 0.2  (warning-grade)
      max_anomaly_score     Highest anomaly_score seen across ANOMALY_CHECKs
      anomaly_band          Worst anomaly band across all metric rows

    Metrics at daily grain
    ───────────────────────
      total_rates_evaluated   Rates that had at least one quality check run
      rates_all_passed        Rates where all_checks_passed = TRUE
      rates_any_failed        Rates where at least one check failed
      daily_pass_rate_pct     rates_all_passed / total_rates_evaluated × 100
      avg_composite_score     Average composite_score across all rates
      null_check_pass_rate    % of rates where NULL_CHECK passed
      range_check_pass_rate   % of rates where RANGE_CHECK passed
      anomaly_check_pass_rate % of rates where ANOMALY_CHECK passed
      rates_with_anomaly      Count of rates that triggered an anomaly
      avg_anomaly_score       Average max_anomaly_score (anomalous rates only)
      quality_trend           IMPROVING / STABLE / DEGRADING vs prior 7-day avg

Downstream  : marts/fct_quality_dashboard.sql
================================================================================
*/

with

metrics as (

    select
        metric_id,
        rate_id,
        check_name,
        check_passed,
        anomaly_score,
        anomaly_band,
        check_category,
        severity_level,
        is_anomaly,
        created_at

    from {{ ref('stg_data_quality_metrics') }}

),

rates as (

    select
        rate_id,
        from_currency_id,
        to_currency_id,
        source_id,
        rate,
        rate_date,
        data_quality_score,
        quality_band,
        is_stale

    from {{ ref('stg_exchange_rates') }}

),

from_currencies as (

    select
        currency_id,
        currency_code   as from_currency_code,
        currency_name   as from_currency_name

    from {{ ref('stg_currencies') }}

),

to_currencies as (

    select
        currency_id,
        currency_code   as to_currency_code,
        currency_name   as to_currency_name

    from {{ ref('stg_currencies') }}

),

sources as (

    select
        source_id,
        source_name,
        source_category

    from {{ ref('stg_api_sources') }}

),

-- =========================================================================
-- LEVEL 1 — rate_id grain: pivot checks into columns
-- =========================================================================

/*
Each rate_id can have up to three metric rows (one per check_name).
We pivot using conditional aggregation so the consumer sees one row per
rate rather than joining three times or using CROSSTAB.
*/
rate_checks_pivoted as (

    select

        rate_id,

        -- ----------------------------------------------------------------
        -- Pivoted check results
        -- NULL means the check was never recorded for this rate (e.g.
        -- anomaly check only runs when the pair has enough history).
        -- ----------------------------------------------------------------
        bool_or(case when check_name = 'NULL_CHECK'    then check_passed end)
                                                            as null_check_passed,
        bool_or(case when check_name = 'RANGE_CHECK'   then check_passed end)
                                                            as range_check_passed,
        bool_or(case when check_name = 'ANOMALY_CHECK' then check_passed end)
                                                            as anomaly_check_passed,

        -- ----------------------------------------------------------------
        -- Check counts
        -- ----------------------------------------------------------------
        count(distinct check_name)                          as checks_run,

        count(*) filter (where check_passed = true)         as checks_passed,

        count(*) filter (where check_passed = false)        as checks_failed,

        -- ----------------------------------------------------------------
        -- Anomaly detail
        -- ----------------------------------------------------------------
        max(anomaly_score)                                  as max_anomaly_score,

        -- Worst anomaly band seen across all checks for this rate
        max(case anomaly_band
            when 'CRITICAL' then 5
            when 'HIGH'     then 4
            when 'MEDIUM'   then 3
            when 'LOW'      then 2
            when 'NONE'     then 1
            else                 0
        end)                                                as worst_anomaly_band_rank,

        bool_or(is_anomaly)                                 as has_anomaly,

        -- ----------------------------------------------------------------
        -- Metadata
        -- ----------------------------------------------------------------
        max(created_at)                                     as last_check_at

    from metrics
    group by rate_id

),

rate_quality as (

    select

        p.rate_id,
        p.null_check_passed,
        p.range_check_passed,
        p.anomaly_check_passed,
        p.checks_run,
        p.checks_passed,
        p.checks_failed,
        p.has_anomaly,
        p.max_anomaly_score,
        p.last_check_at,

        -- ----------------------------------------------------------------
        -- all_checks_passed: TRUE only if every recorded check passed
        -- ----------------------------------------------------------------
        case
            when p.checks_failed = 0 and p.checks_run > 0 then true
            else false
        end                                                 as all_checks_passed,

        -- ----------------------------------------------------------------
        -- Worst anomaly band as a label (reverse the rank encoding above)
        -- ----------------------------------------------------------------
        case p.worst_anomaly_band_rank
            when 5 then 'CRITICAL'
            when 4 then 'HIGH'
            when 3 then 'MEDIUM'
            when 2 then 'LOW'
            when 1 then 'NONE'
            else        'UNKNOWN'
        end                                                 as worst_anomaly_band,

        -- ----------------------------------------------------------------
        -- Composite quality score (weighted average of check outcomes)
        --
        -- Weights:
        --   NULL_CHECK    0.4  — missing data is a hard blocker
        --   RANGE_CHECK   0.4  — out-of-range values corrupt analytics
        --   ANOMALY_CHECK 0.2  — outlier flag (softer; may be genuine)
        --
        -- Each check contributes its weight × (1 if passed, 0 if failed).
        -- Unrecorded checks are excluded from the denominator so the score
        -- is not penalised for checks that were never run.
        -- ----------------------------------------------------------------
        case
            when p.checks_run = 0 then null
            else round(
                (
                    coalesce(p.null_check_passed::int,    0) * 0.4
                  + coalesce(p.range_check_passed::int,   0) * 0.4
                  + coalesce(p.anomaly_check_passed::int, 0) * 0.2
                )
                /
                (
                    case when p.null_check_passed    is not null then 0.4 else 0 end
                  + case when p.range_check_passed   is not null then 0.4 else 0 end
                  + case when p.anomaly_check_passed is not null then 0.2 else 0 end
                ),
                4
            )
        end                                                 as composite_score

    from rate_checks_pivoted p

),

-- Join rate metadata onto the rate-grain quality rows
rate_quality_enriched as (

    select

        -- ----------------------------------------------------------------
        -- Keys
        -- ----------------------------------------------------------------
        r.rate_id,
        r.from_currency_id,
        r.to_currency_id,
        r.source_id,
        r.rate_date,

        -- ----------------------------------------------------------------
        -- Rate context
        -- ----------------------------------------------------------------
        r.rate,
        r.data_quality_score            as extractor_quality_score,
        r.quality_band                  as extractor_quality_band,
        r.is_stale,

        -- ----------------------------------------------------------------
        -- Dimension labels
        -- ----------------------------------------------------------------
        fc.from_currency_code,
        tc.to_currency_code,
        fc.from_currency_code || '_' || tc.to_currency_code
                                        as currency_pair,
        s.source_name,

        -- ----------------------------------------------------------------
        -- Quality check results (rate grain)
        -- ----------------------------------------------------------------
        q.null_check_passed,
        q.range_check_passed,
        q.anomaly_check_passed,
        q.checks_run,
        q.checks_passed,
        q.checks_failed,
        q.all_checks_passed,
        q.composite_score,
        q.has_anomaly,
        q.max_anomaly_score,
        q.worst_anomaly_band,
        q.last_check_at

    from rates                  r
    left join rate_quality      q  on r.rate_id          = q.rate_id
    left join from_currencies   fc on r.from_currency_id = fc.currency_id
    left join to_currencies     tc on r.to_currency_id   = tc.currency_id
    left join sources           s  on r.source_id        = s.source_id

),

-- =========================================================================
-- LEVEL 2 — daily grain: roll up rate_quality_enriched per pair per day
-- =========================================================================
daily_quality as (

    select

        currency_pair,
        from_currency_id,
        to_currency_id,
        source_id,
        source_name,
        rate_date,

        -- ----------------------------------------------------------------
        -- Volume
        -- ----------------------------------------------------------------
        count(*)                                            as total_rates_evaluated,

        count(*) filter (where all_checks_passed = true)    as rates_all_passed,

        count(*) filter (where checks_failed > 0)           as rates_any_failed,

        count(*) filter (where has_anomaly = true)          as rates_with_anomaly,

        -- ----------------------------------------------------------------
        -- Pass rates per check type
        -- ----------------------------------------------------------------
        round(
            count(*) filter (where null_check_passed = true)::numeric
            / nullif(count(*) filter (where null_check_passed is not null), 0)
            * 100,
            2
        )                                                   as null_check_pass_rate,

        round(
            count(*) filter (where range_check_passed = true)::numeric
            / nullif(count(*) filter (where range_check_passed is not null), 0)
            * 100,
            2
        )                                                   as range_check_pass_rate,

        round(
            count(*) filter (where anomaly_check_passed = true)::numeric
            / nullif(count(*) filter (where anomaly_check_passed is not null), 0)
            * 100,
            2
        )                                                   as anomaly_check_pass_rate,

        -- ----------------------------------------------------------------
        -- Overall pass rate and composite score
        -- ----------------------------------------------------------------
        round(
            count(*) filter (where all_checks_passed = true)::numeric
            / nullif(count(*), 0) * 100,
            2
        )                                                   as daily_pass_rate_pct,

        round(avg(composite_score)::numeric, 4)             as avg_composite_score,

        round(avg(extractor_quality_score)::numeric, 4)     as avg_extractor_quality,

        -- ----------------------------------------------------------------
        -- Anomaly summary
        -- ----------------------------------------------------------------
        round(avg(max_anomaly_score) filter (
            where has_anomaly = true
        )::numeric, 4)                                      as avg_anomaly_score,

        max(max_anomaly_score)                              as max_anomaly_score_today,

        -- Worst anomaly band observed today
        mode() within group (
            order by worst_anomaly_band
        ) filter (where has_anomaly = true)                 as dominant_anomaly_band,

        -- ----------------------------------------------------------------
        -- Stale data
        -- ----------------------------------------------------------------
        count(*) filter (where is_stale = true)             as stale_rates_count,

        round(
            count(*) filter (where is_stale = true)::numeric
            / nullif(count(*), 0) * 100,
            2
        )                                                   as stale_rate_pct

    from rate_quality_enriched
    group by
        currency_pair,
        from_currency_id,
        to_currency_id,
        source_id,
        source_name,
        rate_date

),

-- -------------------------------------------------------------------------
-- Add 7-day rolling average of composite score for trend detection
-- -------------------------------------------------------------------------
with_trend as (

    select

        *,

        avg(avg_composite_score) over (
            partition by currency_pair, source_name
            order by rate_date
            rows between 7 preceding and 1 preceding
        )                                                   as prior_7d_avg_score,

        avg(daily_pass_rate_pct) over (
            partition by currency_pair, source_name
            order by rate_date
            rows between 7 preceding and 1 preceding
        )                                                   as prior_7d_pass_rate

    from daily_quality

),

final as (

    select

        -- ----------------------------------------------------------------
        -- Surrogate key
        -- ----------------------------------------------------------------
        {{
            dbt_utils.generate_surrogate_key([
                'currency_pair',
                'source_id',
                'rate_date'
            ])
        }}                                                  as quality_summary_id,

        -- ----------------------------------------------------------------
        -- Dimensions
        -- ----------------------------------------------------------------
        currency_pair,
        from_currency_id,
        to_currency_id,
        source_id,
        source_name,
        rate_date,
        to_char(rate_date, 'YYYY-MM')                       as year_month,
        extract(year  from rate_date)::int                  as year_number,
        extract(month from rate_date)::int                  as month_number,
        extract(week  from rate_date)::int                  as week_of_year,

        -- ----------------------------------------------------------------
        -- Volume
        -- ----------------------------------------------------------------
        total_rates_evaluated,
        rates_all_passed,
        rates_any_failed,
        rates_with_anomaly,
        stale_rates_count,

        -- ----------------------------------------------------------------
        -- Pass rates
        -- ----------------------------------------------------------------
        null_check_pass_rate,
        range_check_pass_rate,
        anomaly_check_pass_rate,
        daily_pass_rate_pct,
        stale_rate_pct,

        -- ----------------------------------------------------------------
        -- Composite scores
        -- ----------------------------------------------------------------
        avg_composite_score,
        avg_extractor_quality,
        prior_7d_avg_score,
        prior_7d_pass_rate,

        -- ----------------------------------------------------------------
        -- Anomaly
        -- ----------------------------------------------------------------
        avg_anomaly_score,
        max_anomaly_score_today,
        dominant_anomaly_band,

        -- ----------------------------------------------------------------
        -- Quality trend signal
        --
        -- Compares today's avg_composite_score to the prior 7-day average:
        --   IMPROVING  : today > prior_7d + 0.02  (≥2pp improvement)
        --   DEGRADING  : today < prior_7d – 0.02  (≥2pp degradation)
        --   STABLE     : within ±0.02 of prior average
        --   INSUFFICIENT_DATA : fewer than 7 prior days of history
        -- ----------------------------------------------------------------
        case
            when prior_7d_avg_score is null             then 'INSUFFICIENT_DATA'
            when avg_composite_score > prior_7d_avg_score + 0.02
                                                        then 'IMPROVING'
            when avg_composite_score < prior_7d_avg_score - 0.02
                                                        then 'DEGRADING'
            else                                             'STABLE'
        end                                                 as quality_trend,

        -- ----------------------------------------------------------------
        -- Alert flag: today's pass rate dropped below SLA threshold
        -- ----------------------------------------------------------------
        case
            when daily_pass_rate_pct < {{ var('sla_quality_pass_rate', 90) }}
            then true
            else false
        end                                                 as quality_sla_breached,

        -- ----------------------------------------------------------------
        -- Metadata
        -- ----------------------------------------------------------------
        current_timestamp                                   as dbt_updated_at

    from with_trend

)

select * from final