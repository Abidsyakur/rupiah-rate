/*
================================================================================
fct_exchange_rates.sql
================================================================================
Layer       : Marts (Fact)
Upstream    : {{ ref('stg_exchange_rates') }}
             {{ ref('dim_currencies') }}    (from + to)
             {{ ref('dim_api_sources') }}
             {{ ref('int_quality_summary') }}
Materialized: table
Description :
    Tick-level (intraday) exchange rate fact table — the most granular
    business-ready table in the marts layer. One row per raw observation,
    fully denormalised with dimension labels and quality flags so BI tools
    can query this table directly without further joins.

Grain       : One row per rate_id (same grain as stg_exchange_rates)
Used by     : Dashboards needing intraday detail, ad-hoc analysis,
             drill-through from fct_daily_snapshots
================================================================================
*/

with

rates as (

    select
        rate_id,
        from_currency_id,
        to_currency_id,
        source_id,
        rate,
        rate_timestamp,
        rate_date,
        rate_hour,
        day_of_week,
        week_of_year,
        month_number,
        year_number,
        year_month,
        data_quality_score,
        quality_band,
        is_stale,
        is_business_day,
        rate_decimal_places,
        created_at

    from {{ ref('stg_exchange_rates') }}

),

from_currency as (

    select
        currency_id,
        currency_code   as from_currency_code,
        currency_name   as from_currency_name,
        region          as from_region

    from {{ ref('dim_currencies') }}

),

to_currency as (

    select
        currency_id,
        currency_code   as to_currency_code,
        currency_name   as to_currency_name,
        region          as to_region

    from {{ ref('dim_currencies') }}

),

sources as (

    select
        source_id,
        source_name,
        source_category,
        health_status   as source_health_status

    from {{ ref('dim_api_sources') }}

),

-- -------------------------------------------------------------------------
-- Quality summary lookup (daily grain) — joined down to rate-level via
-- pair + source + date so each tick inherits its day's quality context.
-- -------------------------------------------------------------------------
quality as (

    select
        currency_pair,
        source_id,
        rate_date,
        daily_pass_rate_pct,
        avg_composite_score,
        quality_trend,
        quality_sla_breached

    from {{ ref('int_quality_summary') }}

),

final as (

    select

        -- ----------------------------------------------------------------
        -- Primary key
        -- ----------------------------------------------------------------
        r.rate_id,

        -- ----------------------------------------------------------------
        -- Foreign keys (to dim_currencies, dim_api_sources)
        -- ----------------------------------------------------------------
        r.from_currency_id,
        r.to_currency_id,
        r.source_id,

        -- ----------------------------------------------------------------
        -- Dimension labels (denormalised for BI tool convenience)
        -- ----------------------------------------------------------------
        fc.from_currency_code,
        fc.from_currency_name,
        fc.from_region,
        tc.to_currency_code,
        tc.to_currency_name,
        tc.to_region,
        fc.from_currency_code || '_' || tc.to_currency_code
                                                            as currency_pair,
        s.source_name,
        s.source_category,
        s.source_health_status,

        -- ----------------------------------------------------------------
        -- Core measure
        -- ----------------------------------------------------------------
        r.rate,
        r.rate_decimal_places,

        -- ----------------------------------------------------------------
        -- Time dimensions
        -- ----------------------------------------------------------------
        r.rate_timestamp,
        r.rate_date,
        r.rate_hour,
        r.day_of_week,
        r.week_of_year,
        r.month_number,
        r.year_number,
        r.year_month,
        r.is_business_day,

        -- ----------------------------------------------------------------
        -- Tick-level quality flags
        -- ----------------------------------------------------------------
        r.data_quality_score,
        r.quality_band,
        r.is_stale,

        -- ----------------------------------------------------------------
        -- Daily quality context (inherited from int_quality_summary)
        -- ----------------------------------------------------------------
        q.daily_pass_rate_pct,
        q.avg_composite_score              as daily_avg_composite_score,
        q.quality_trend                    as daily_quality_trend,
        coalesce(q.quality_sla_breached, false)
                                            as daily_quality_sla_breached,

        -- ----------------------------------------------------------------
        -- Business-ready quality flag — single field for dashboard filters
        -- ----------------------------------------------------------------

        /*
        Consolidates tick-level and daily-level quality signals into one
        traffic-light flag for simple dashboard filtering:
          RED    — this tick is stale OR the day's quality SLA was breached
          YELLOW — quality_band is LOW or MEDIUM (degraded but usable)
          GREEN  — quality_band is HIGH and no other concerns
        */
        case
            when r.is_stale = true
                 or coalesce(q.quality_sla_breached, false) = true
            then 'RED'
            when r.quality_band in ('LOW', 'MEDIUM')
            then 'YELLOW'
            else 'GREEN'
        end                                                 as quality_flag,

        -- ----------------------------------------------------------------
        -- Audit
        -- ----------------------------------------------------------------
        r.created_at,
        current_timestamp                                   as dbt_updated_at

    from rates                  r
    inner join from_currency    fc on r.from_currency_id = fc.currency_id
    inner join to_currency      tc on r.to_currency_id   = tc.currency_id
    inner join sources          s  on r.source_id        = s.source_id
    left join  quality          q
        on  fc.from_currency_code || '_' || tc.to_currency_code = q.currency_pair
        and r.source_id  = q.source_id
        and r.rate_date  = q.rate_date

)

select * from final