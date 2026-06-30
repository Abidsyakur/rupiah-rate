/*
================================================================================
exchange_rate_analysis.sql
================================================================================
Type        : dbt analysis (compiled, NOT materialized as a table/view)
Upstream    : {{ ref('fct_daily_snapshots') }}
             {{ ref('dim_currencies') }}
             {{ ref('dim_api_sources') }}
Description :
    Ad-hoc analytical query: ranks currency pairs by recent volatility and
    trend strength, surfacing the most actionable pairs for a daily
    market-summary report.

    This is NOT part of the persisted DAG — `dbt run` will not build this
    file. Use `dbt compile` to render the final SQL (output appears under
    target/compiled/.../analyses/exchange_rate_analysis.sql) and run it
    manually, or paste the compiled SQL into a BI tool / notebook.

    Question answered:
    "Which currency pairs moved the most in the last 7 days, and are they
    currently trending up or down with healthy data quality?"

    Output columns:
      currency_pair, source_name, region
      latest_close, latest_date
      avg_volatility_7d        Average of volatility_7d over the last 7 days
      cumulative_change_pct    Compounded % change over the last 7 days
      trend_label               Most recent day's trend_label
      quality_flag              Most recent day's quality_flag
      volatility_rank           Rank 1 = most volatile pair in the window
================================================================================
*/

with

recent_snapshots as (

    select
        currency_pair,
        from_currency_id,
        to_currency_id,
        source_id,
        source_name,
        rate_date,
        rate_close,
        daily_change_pct,
        volatility_7d,
        trend_label,
        quality_flag,
        volatility_flag

    from {{ ref('fct_daily_snapshots') }}
    where rate_date >= current_date - interval '7 days'

),

from_currencies as (

    select
        currency_id,
        currency_code   as from_currency_code,
        region          as from_region

    from {{ ref('dim_currencies') }}

),

sources as (

    select
        source_id,
        source_category

    from {{ ref('dim_api_sources') }}

),

-- -------------------------------------------------------------------------
-- Step 1: aggregate the 7-day window per pair + source
-- -------------------------------------------------------------------------
pair_summary as (

    select

        currency_pair,
        from_currency_id,
        to_currency_id,
        source_id,
        source_name,

        max(rate_date)                                  as latest_date,
        avg(volatility_7d)                               as avg_volatility_7d,

        /*
        Cumulative percentage change over the window, computed as the
        product of (1 + daily_change_pct/100) across all days minus 1,
        which correctly compounds rather than naively summing percentages.
        */
        (
            exp(sum(ln(1 + (daily_change_pct / 100.0)))) - 1
        ) * 100                                          as cumulative_change_pct,

        count(*)                                          as days_in_window

    from recent_snapshots
    where daily_change_pct is not null
      and daily_change_pct > -100   -- guard against ln() domain error
    group by
        currency_pair,
        from_currency_id,
        to_currency_id,
        source_id,
        source_name

),

-- -------------------------------------------------------------------------
-- Step 2: attach the MOST RECENT day's close, trend, and quality flag
-- (these are point-in-time fields, not aggregated)
-- -------------------------------------------------------------------------
latest_day as (

    select
        currency_pair,
        source_id,
        rate_close,
        trend_label,
        quality_flag,
        volatility_flag,

        row_number() over (
            partition by currency_pair, source_id
            order by rate_date desc
        ) as rn

    from recent_snapshots

),

-- -------------------------------------------------------------------------
-- Step 3: combine, enrich with dimensions, and rank by volatility
-- -------------------------------------------------------------------------
final as (

    select

        ps.currency_pair,
        ps.source_name,
        fc.from_region                                   as region,

        ld.rate_close                                    as latest_close,
        ps.latest_date,

        round(ps.avg_volatility_7d::numeric, 4)          as avg_volatility_7d,
        round(ps.cumulative_change_pct::numeric, 4)      as cumulative_change_pct,
        ps.days_in_window,

        ld.trend_label,
        ld.quality_flag,
        ld.volatility_flag,

        s.source_category,

        -- Rank pairs by volatility, most volatile first
        rank() over (
            order by ps.avg_volatility_7d desc nulls last
        )                                                  as volatility_rank

    from pair_summary       ps
    inner join latest_day   ld
        on  ps.currency_pair = ld.currency_pair
        and ps.source_id     = ld.source_id
        and ld.rn = 1
    inner join from_currencies fc
        on ps.from_currency_id = fc.currency_id
    inner join sources       s
        on ps.source_id = s.source_id

)

select *
from final
order by volatility_rank