/*
================================================================================
fct_daily_snapshots.sql
================================================================================
Layer       : Marts (Fact)
Upstream    : {{ ref('int_exchange_rate_statistics') }}
             {{ ref('int_quality_summary') }}
             {{ ref('dim_currencies') }}     (from + to)
             {{ ref('dim_api_sources') }}
Materialized: table
Description :
    Daily-grain fact table — the primary table for dashboards and reports.
    Combines OHLC, technical indicators (moving averages, Bollinger Bands,
    momentum, volatility), and daily quality KPIs into one fully
    denormalised, business-ready row per currency pair per day per source.

    This mirrors the `daily_snapshots` table structure from ADR-002
    (docs/SCHEMA.md) but is generated entirely via dbt from the
    intermediate layer rather than written directly by the ETL pipeline,
    giving analysts a transformation-traceable equivalent for BI use.

Grain       : One row per (currency_pair, source_id, rate_date)
Used by     : Primary dashboard table — daily trend charts, anomaly alerts,
             executive summary reports
================================================================================
*/

with

stats as (

    select
        daily_rate_id,
        rate_date,
        currency_pair,
        from_currency_id,
        to_currency_id,
        source_id,
        source_name,
        rate_open,
        rate_high,
        rate_low,
        rate_close,
        rate_avg,
        tick_count,
        daily_change,
        daily_change_pct,
        daily_range,
        ma_7d,
        ma_14d,
        ma_30d,
        bb_middle,
        bb_upper,
        bb_lower,
        bb_width,
        bb_position,
        volatility_7d,
        volatility_30d,
        atr_14d,
        roc_7d,
        roc_30d,
        above_ma_7d,
        above_ma_30d,
        golden_cross,
        death_cross,
        days_of_history

    from {{ ref('int_exchange_rate_statistics') }}

),

quality as (

    select
        currency_pair,
        source_id,
        rate_date,
        total_rates_evaluated,
        daily_pass_rate_pct,
        avg_composite_score,
        rates_with_anomaly,
        avg_anomaly_score,
        dominant_anomaly_band,
        quality_trend,
        quality_sla_breached,
        stale_rate_pct

    from {{ ref('int_quality_summary') }}

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
        currency_name   as to_currency_name

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

final as (

    select

        -- ----------------------------------------------------------------
        -- Surrogate key (same key as int_daily_exchange_rates)
        -- ----------------------------------------------------------------
        st.daily_rate_id                                    as snapshot_id,

        -- ----------------------------------------------------------------
        -- Dimensions
        -- ----------------------------------------------------------------
        st.rate_date,
        st.from_currency_id,
        st.to_currency_id,
        fc.from_currency_code,
        fc.from_currency_name,
        fc.from_region,
        tc.to_currency_code,
        tc.to_currency_name,
        st.currency_pair,
        st.source_id,
        st.source_name,
        s.source_category,
        s.source_health_status,

        -- ----------------------------------------------------------------
        -- OHLC
        -- ----------------------------------------------------------------
        st.rate_open,
        st.rate_high,
        st.rate_low,
        st.rate_close,
        st.rate_avg,
        st.tick_count,

        -- ----------------------------------------------------------------
        -- Daily change metrics
        -- ----------------------------------------------------------------
        st.daily_change,
        st.daily_change_pct,
        st.daily_range,

        -- ----------------------------------------------------------------
        -- Technical indicators
        -- ----------------------------------------------------------------
        st.ma_7d,
        st.ma_14d,
        st.ma_30d,
        st.bb_middle,
        st.bb_upper,
        st.bb_lower,
        st.bb_width,
        st.bb_position,
        st.volatility_7d,
        st.volatility_30d,
        st.atr_14d,
        st.roc_7d,
        st.roc_30d,

        -- ----------------------------------------------------------------
        -- Trend signals
        -- ----------------------------------------------------------------
        st.above_ma_7d,
        st.above_ma_30d,
        st.golden_cross,
        st.death_cross,
        st.days_of_history,

        /*
        Business-friendly trend label combining the cross signals into a
        single categorical field for dashboard cards:
          BULLISH_CROSS   golden_cross fired today
          BEARISH_CROSS   death_cross fired today
          UPTREND         above both MAs, no cross today
          DOWNTREND       below both MAs, no cross today
          MIXED           above one MA but not the other
        */
        case
            when st.golden_cross = true                          then 'BULLISH_CROSS'
            when st.death_cross  = true                          then 'BEARISH_CROSS'
            when st.above_ma_7d  = true and st.above_ma_30d = true
                                                                  then 'UPTREND'
            when st.above_ma_7d  = false and st.above_ma_30d = false
                                                                  then 'DOWNTREND'
            else                                                       'MIXED'
        end                                                 as trend_label,

        -- ----------------------------------------------------------------
        -- Quality KPIs (daily grain)
        -- ----------------------------------------------------------------
        q.total_rates_evaluated,
        q.daily_pass_rate_pct,
        q.avg_composite_score,
        q.rates_with_anomaly,
        q.avg_anomaly_score,
        q.dominant_anomaly_band,
        q.quality_trend,
        coalesce(q.quality_sla_breached, false)             as quality_sla_breached,
        q.stale_rate_pct,

        -- ----------------------------------------------------------------
        -- Business-ready quality flag — single field for dashboard filters
        -- ----------------------------------------------------------------

        /*
        Traffic-light summary combining quality SLA and anomaly presence:
          RED    — quality SLA breached, OR more than 10% of ticks anomalous
          YELLOW — some anomalies present (≤10%) or pass rate 80–90%
          GREEN  — no SLA breach, low/no anomalies
        */
        case
            when coalesce(q.quality_sla_breached, false) = true
                 or coalesce(q.rates_with_anomaly, 0)::numeric
                    / nullif(q.total_rates_evaluated, 0) > 0.10
            then 'RED'
            when coalesce(q.rates_with_anomaly, 0) > 0
                 or coalesce(q.daily_pass_rate_pct, 100) < 90
            then 'YELLOW'
            else 'GREEN'
        end                                                 as quality_flag,

        -- ----------------------------------------------------------------
        -- Business-ready volatility flag
        -- ----------------------------------------------------------------

        /*
        Classifies the day's volatility regime using volatility_7d
        (std dev of daily % change). Thresholds tuned for FX majors;
        revisit if tracking historically more volatile pairs.
          HIGH    volatility_7d > 1.5%
          MODERATE volatility_7d 0.5% – 1.5%
          LOW     volatility_7d < 0.5%
          UNKNOWN insufficient history (volatility_7d is NULL)
        */
        case
            when st.volatility_7d is null      then 'UNKNOWN'
            when st.volatility_7d > 1.5         then 'HIGH'
            when st.volatility_7d >= 0.5         then 'MODERATE'
            else                                      'LOW'
        end                                                 as volatility_flag,

        -- ----------------------------------------------------------------
        -- Calendar helpers
        -- ----------------------------------------------------------------
        extract(year  from st.rate_date)::int               as year_number,
        extract(month from st.rate_date)::int                as month_number,
        extract(week  from st.rate_date)::int                as week_of_year,
        extract(isodow from st.rate_date)::int               as day_of_week,
        to_char(st.rate_date, 'YYYY-MM')                     as year_month,

        -- ----------------------------------------------------------------
        -- Metadata
        -- ----------------------------------------------------------------
        current_timestamp                                   as dbt_updated_at

    from stats                   st
    inner join from_currency     fc on st.from_currency_id = fc.currency_id
    inner join to_currency       tc on st.to_currency_id   = tc.currency_id
    inner join sources           s  on st.source_id        = s.source_id
    left join  quality           q
        on st.currency_pair = q.currency_pair
        and st.source_id    = q.source_id
        and st.rate_date    = q.rate_date

)

select * from final