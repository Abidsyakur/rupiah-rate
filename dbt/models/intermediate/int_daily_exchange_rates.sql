/*
================================================================================
int_daily_exchange_rates.sql
================================================================================
Layer       : Intermediate
Upstream    : {{ ref('stg_exchange_rates') }}
             {{ ref('stg_currencies') }}      (from_currency)
             {{ ref('stg_currencies') }}      (to_currency)
             {{ ref('stg_api_sources') }}
Materialized: table
Description :
    Aggregates intraday exchange rate ticks into daily OHLCV-style summaries.

    Calculations per (from_currency, to_currency, source, rate_date):
    ─────────────────────────────────────────────────────────────────
    OHLC
      rate_open     First rate observed on that day (chronological)
      rate_high     Highest rate observed on that day
      rate_low      Lowest rate observed on that day
      rate_close    Last rate observed on that day (chronological)

    Volume proxy (no true volume in FX — we use observation count)
      tick_count    Number of individual rate ticks that day

    Change metrics
      daily_change        rate_close – rate_open (absolute)
      daily_change_pct    (rate_close – rate_open) / rate_open * 100
      daily_range         rate_high – rate_low (day's spread)
      daily_range_pct     daily_range / rate_open * 100

    Quality
      avg_quality_score   Average data_quality_score across the day's ticks
      min_quality_score   Worst quality score seen that day
      has_stale_ticks     TRUE if any tick was flagged is_stale

Downstream  : int_exchange_rate_statistics.sql, marts/fct_daily_rates.sql
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
        data_quality_score,
        is_stale,
        quality_band

    from {{ ref('stg_exchange_rates') }}

),

from_currencies as (

    select
        currency_id,
        currency_code     as from_currency_code,
        currency_name     as from_currency_name,
        currency_label    as from_currency_label

    from {{ ref('stg_currencies') }}

),

to_currencies as (

    select
        currency_id,
        currency_code     as to_currency_code,
        currency_name     as to_currency_name,
        currency_label    as to_currency_label

    from {{ ref('stg_currencies') }}

),

sources as (

    select
        source_id,
        source_name,
        source_category

    from {{ ref('stg_api_sources') }}

),

-- -------------------------------------------------------------------------
-- Step 1: attach first/last rate per (pair, source, date) via window fns
-- -------------------------------------------------------------------------
rates_with_ordinals as (

    select
        *,
        /*
        Assign chronological rank within each (pair, source, date) group.
        FIRST_VALUE / LAST_VALUE would also work but ROW_NUMBER + filter
        is more portable across dbt-supported databases.
        */
        row_number() over (
            partition by from_currency_id, to_currency_id, source_id, rate_date
            order by rate_timestamp asc
        ) as tick_rank_asc,

        row_number() over (
            partition by from_currency_id, to_currency_id, source_id, rate_date
            order by rate_timestamp desc
        ) as tick_rank_desc,

        count(*) over (
            partition by from_currency_id, to_currency_id, source_id, rate_date
        ) as tick_count_total

    from rates

),

-- -------------------------------------------------------------------------
-- Step 2: extract open and close per group
-- -------------------------------------------------------------------------
open_rates as (

    select
        from_currency_id,
        to_currency_id,
        source_id,
        rate_date,
        rate as rate_open

    from rates_with_ordinals
    where tick_rank_asc = 1

),

close_rates as (

    select
        from_currency_id,
        to_currency_id,
        source_id,
        rate_date,
        rate as rate_close

    from rates_with_ordinals
    where tick_rank_desc = 1

),

-- -------------------------------------------------------------------------
-- Step 3: aggregate HLC + quality + metadata per group
-- -------------------------------------------------------------------------
daily_agg as (

    select
        from_currency_id,
        to_currency_id,
        source_id,
        rate_date,

        max(rate)                       as rate_high,
        min(rate)                       as rate_low,
        avg(rate)                       as rate_avg,
        max(tick_count_total)           as tick_count,

        avg(data_quality_score)         as avg_quality_score,
        min(data_quality_score)         as min_quality_score,

        -- TRUE if ANY tick in the day was stale
        bool_or(is_stale)              as has_stale_ticks,

        -- Dominant quality band (most common band among the day's ticks)
        mode() within group (
            order by quality_band
        )                               as dominant_quality_band

    from rates_with_ordinals
    group by
        from_currency_id,
        to_currency_id,
        source_id,
        rate_date

),

-- -------------------------------------------------------------------------
-- Step 4: join open, close, and aggregates together
-- -------------------------------------------------------------------------
daily_ohlc as (
    select
        d.from_currency_id,
        d.to_currency_id,
        d.source_id,
        d.rate_date,
        o.rate_open,
        d.rate_high,
        d.rate_low,
        c.rate_close,
        d.rate_avg,
        d.tick_count,
        d.avg_quality_score,
        d.min_quality_score,
        d.has_stale_ticks,
        d.dominant_quality_band,

        -- ----------------------------------------------------------------
        -- Change metrics (all relative to open)
        -- ----------------------------------------------------------------

        -- Absolute close-to-open change
        round(c.rate_close - o.rate_open, 6)
                                        as daily_change,

        -- Percentage close-to-open change (NULL if open is zero)
        case
            when o.rate_open = 0 then null
            else round(
                (c.rate_close - o.rate_open) / o.rate_open * 100,
                4
            )
        end                             as daily_change_pct,

        -- Intraday spread (high – low)
        round(d.rate_high - d.rate_low, 6)
                                        as daily_range,

        -- Intraday spread as % of open
        case
            when o.rate_open = 0 then null
            else round(
                (d.rate_high - d.rate_low) / o.rate_open * 100,
                4
            )
        end                             as daily_range_pct,

        -- Direction of the day (UP / DOWN / FLAT)
        case
            when c.rate_close > o.rate_open then 'UP'
            when c.rate_close < o.rate_open then 'DOWN'
            else                                 'FLAT'
        end                             as daily_direction

    from daily_agg           d
    inner join open_rates    o using (from_currency_id, to_currency_id, source_id, rate_date)
    inner join close_rates   c using (from_currency_id, to_currency_id, source_id, rate_date)

),

-- -------------------------------------------------------------------------
-- Step 5: enrich with dimension labels
-- -------------------------------------------------------------------------
final as (

    select

        -- ----------------------------------------------------------------
        -- Surrogate / natural keys
        -- ----------------------------------------------------------------
        {{
            dbt_utils.generate_surrogate_key([
                'd.from_currency_id',
                'd.to_currency_id',
                'd.source_id',
                'd.rate_date'
            ])
        }}                              as daily_rate_id,

        d.rate_date,
        d.from_currency_id,
        d.to_currency_id,
        d.source_id,

        -- ----------------------------------------------------------------
        -- Currency dimension labels
        -- ----------------------------------------------------------------
        fc.from_currency_code,
        fc.from_currency_name,
        tc.to_currency_code,
        tc.to_currency_name,

        -- Canonical pair string, e.g. "USD_IDR"
        fc.from_currency_code || '_' || tc.to_currency_code
                                        as currency_pair,

        -- ----------------------------------------------------------------
        -- Source labels
        -- ----------------------------------------------------------------
        s.source_name,
        s.source_category,

        -- ----------------------------------------------------------------
        -- OHLCV
        -- ----------------------------------------------------------------
        d.rate_open,
        d.rate_high,
        d.rate_low,
        d.rate_close,
        round(d.rate_avg::numeric, 6)   as rate_avg,
        d.tick_count,

        -- ----------------------------------------------------------------
        -- Change metrics
        -- ----------------------------------------------------------------
        d.daily_change,
        d.daily_change_pct,
        d.daily_range,
        d.daily_range_pct,
        d.daily_direction,

        -- ----------------------------------------------------------------
        -- Quality
        -- ----------------------------------------------------------------
        round(d.avg_quality_score::numeric, 4)  as avg_quality_score,
        d.min_quality_score,
        d.has_stale_ticks,
        d.dominant_quality_band,

        -- ----------------------------------------------------------------
        -- Calendar helpers (for joins / partitioning)
        -- ----------------------------------------------------------------
        extract(year  from d.rate_date)::int    as year_number,
        extract(month from d.rate_date)::int    as month_number,
        extract(week  from d.rate_date)::int    as week_of_year,
        extract(isodow from d.rate_date)::int   as day_of_week,
        to_char(d.rate_date, 'YYYY-MM')         as year_month,

        -- ----------------------------------------------------------------
        -- Metadata
        -- ----------------------------------------------------------------
        current_timestamp                       as dbt_updated_at

    from daily_ohlc          d
    inner join from_currencies fc
        on d.from_currency_id = fc.currency_id
    inner join to_currencies   tc
        on d.to_currency_id = tc.currency_id
    inner join sources         s
        on d.source_id = s.source_id

)

select * from final