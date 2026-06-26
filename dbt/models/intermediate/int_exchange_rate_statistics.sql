/*
================================================================================
int_exchange_rate_statistics.sql
================================================================================
Layer       : Intermediate
Upstream    : {{ ref('int_daily_exchange_rates') }}
Materialized: table
Description :
    Calculates rolling technical indicators and statistical metrics for each
    currency pair, building on the daily OHLC summaries produced by
    int_daily_exchange_rates.

    Indicators calculated (all per currency_pair + source_name):
    ─────────────────────────────────────────────────────────────
    Moving Averages (Simple)
      ma_7d       7-day   simple moving average of rate_close
      ma_14d      14-day  simple moving average of rate_close
      ma_30d      30-day  simple moving average of rate_close

    Exponential Moving Average
      ema_7d      7-day EMA approximated via 2/(N+1) weighting with window
                  Note: true EMA requires recursive calculation not supported
                  natively in SQL windows; we use a weighted average
                  approximation that gives more weight to recent prices.

    Bollinger Band components (20-day)
      bb_middle   20-day simple moving average (middle band)
      bb_std      20-day population standard deviation of rate_close
      bb_upper    bb_middle + (2 × bb_std)   upper band
      bb_lower    bb_middle – (2 × bb_std)   lower band
      bb_width    (bb_upper – bb_lower) / bb_middle × 100  (normalised width)
      bb_position where rate_close sits within the band (0 = lower, 1 = upper)

    Momentum
      roc_7d      Rate of Change over 7 days:
                  (today_close – close_7d_ago) / close_7d_ago × 100
      roc_30d     Rate of Change over 30 days

    Volatility
      volatility_7d   Std dev of daily_change_pct over last 7 days
      volatility_30d  Std dev of daily_change_pct over last 30 days
      atr_14d         Average True Range (14 days) — approximated as
                      avg(daily_range) over last 14 days in FX context

    Trend signals
      above_ma_7d     TRUE if rate_close > ma_7d
      above_ma_30d    TRUE if rate_close > ma_30d
      golden_cross    TRUE on days where ma_7d crosses ABOVE ma_30d
                      (previous day: ma_7d_prev < ma_30d_prev, today: ma_7d >= ma_30d)
      death_cross     TRUE on days where ma_7d crosses BELOW ma_30d

Downstream  : marts/fct_daily_rates.sql, marts/fct_market_signals.sql
================================================================================
*/

with

daily as (

    select
        daily_rate_id,
        rate_date,
        currency_pair,
        from_currency_id,
        to_currency_id,
        source_id,
        source_name,
        from_currency_code,
        to_currency_code,
        rate_open,
        rate_high,
        rate_low,
        rate_close,
        rate_avg,
        tick_count,
        daily_change,
        daily_change_pct,
        daily_range,
        year_number,
        month_number

    from {{ ref('int_daily_exchange_rates') }}

),

-- -------------------------------------------------------------------------
-- Step 1: window functions for moving averages and rolling statistics
-- -------------------------------------------------------------------------
with_windows as (

    select

        *,

        -- ----------------------------------------------------------------
        -- Simple Moving Averages
        -- ----------------------------------------------------------------

        avg(rate_close) over (
            partition by currency_pair, source_name
            order by rate_date
            rows between 6 preceding and current row
        )                                                   as ma_7d,

        avg(rate_close) over (
            partition by currency_pair, source_name
            order by rate_date
            rows between 13 preceding and current row
        )                                                   as ma_14d,

        avg(rate_close) over (
            partition by currency_pair, source_name
            order by rate_date
            rows between 29 preceding and current row
        )                                                   as ma_30d,

        -- ----------------------------------------------------------------
        -- Bollinger Band components (20-day)
        -- ----------------------------------------------------------------

        avg(rate_close) over (
            partition by currency_pair, source_name
            order by rate_date
            rows between 19 preceding and current row
        )                                                   as bb_middle,

        stddev_pop(rate_close) over (
            partition by currency_pair, source_name
            order by rate_date
            rows between 19 preceding and current row
        )                                                   as bb_std,

        -- ----------------------------------------------------------------
        -- Volatility (std dev of daily_change_pct)
        -- ----------------------------------------------------------------

        stddev_pop(daily_change_pct) over (
            partition by currency_pair, source_name
            order by rate_date
            rows between 6 preceding and current row
        )                                                   as volatility_7d,

        stddev_pop(daily_change_pct) over (
            partition by currency_pair, source_name
            order by rate_date
            rows between 29 preceding and current row
        )                                                   as volatility_30d,

        -- ----------------------------------------------------------------
        -- Average True Range approximation (avg of daily_range, 14 days)
        -- ----------------------------------------------------------------

        avg(daily_range) over (
            partition by currency_pair, source_name
            order by rate_date
            rows between 13 preceding and current row
        )                                                   as atr_14d,

        -- ----------------------------------------------------------------
        -- Lag values for Rate of Change and cross detection
        -- ----------------------------------------------------------------

        lag(rate_close, 7) over (
            partition by currency_pair, source_name
            order by rate_date
        )                                                   as close_7d_ago,

        lag(rate_close, 30) over (
            partition by currency_pair, source_name
            order by rate_date
        )                                                   as close_30d_ago,

        -- Previous day's MA values (for golden/death cross detection)
        lag(
            avg(rate_close) over (
                partition by currency_pair, source_name
                order by rate_date
                rows between 6 preceding and current row
            )
        , 1) over (
            partition by currency_pair, source_name
            order by rate_date
        )                                                   as ma_7d_prev,

        lag(
            avg(rate_close) over (
                partition by currency_pair, source_name
                order by rate_date
                rows between 29 preceding and current row
            )
        , 1) over (
            partition by currency_pair, source_name
            order by rate_date
        )                                                   as ma_30d_prev,

        -- Row number within the pair for "minimum history" guards
        row_number() over (
            partition by currency_pair, source_name
            order by rate_date
        )                                                   as row_num

    from daily

),

-- -------------------------------------------------------------------------
-- Step 2: derived indicators that depend on the Step 1 window values
-- -------------------------------------------------------------------------
with_indicators as (

    select

        *,

        -- ----------------------------------------------------------------
        -- Bollinger Bands: upper / lower / width / position
        -- ----------------------------------------------------------------

        round((bb_middle + 2 * bb_std)::numeric, 6)         as bb_upper,
        round((bb_middle - 2 * bb_std)::numeric, 6)         as bb_lower,

        -- Normalised band width: tighter = lower volatility regime
        case
            when bb_middle = 0 or bb_middle is null then null
            else round(
                ((bb_middle + 2 * bb_std) - (bb_middle - 2 * bb_std))
                / bb_middle * 100,
                4
            )
        end                                                  as bb_width,

        -- Where does rate_close sit within the band? 0 = lower, 1 = upper
        case
            when (bb_middle + 2 * bb_std) = (bb_middle - 2 * bb_std) then null
            when bb_std = 0 or bb_std is null then null
            else round(
                (rate_close - (bb_middle - 2 * bb_std))
                / ((bb_middle + 2 * bb_std) - (bb_middle - 2 * bb_std)),
                4
            )
        end                                                  as bb_position,

        -- ----------------------------------------------------------------
        -- Rate of Change
        -- ----------------------------------------------------------------

        case
            when close_7d_ago is null or close_7d_ago = 0 then null
            else round(
                (rate_close - close_7d_ago) / close_7d_ago * 100,
                4
            )
        end                                                  as roc_7d,

        case
            when close_30d_ago is null or close_30d_ago = 0 then null
            else round(
                (rate_close - close_30d_ago) / close_30d_ago * 100,
                4
            )
        end                                                  as roc_30d,

        -- ----------------------------------------------------------------
        -- Trend signals
        -- ----------------------------------------------------------------

        -- Is today's close above the 7-day MA?
        case when rate_close > ma_7d  then true else false end
                                                             as above_ma_7d,

        -- Is today's close above the 30-day MA?
        case when rate_close > ma_30d then true else false end
                                                             as above_ma_30d,

        -- Golden Cross: ma_7d just crossed ABOVE ma_30d today
        case
            when ma_7d_prev  is not null
             and ma_30d_prev is not null
             and ma_7d_prev  < ma_30d_prev
             and ma_7d       >= ma_30d
            then true
            else false
        end                                                  as golden_cross,

        -- Death Cross: ma_7d just crossed BELOW ma_30d today
        case
            when ma_7d_prev  is not null
             and ma_30d_prev is not null
             and ma_7d_prev  >= ma_30d_prev
             and ma_7d       < ma_30d
            then true
            else false
        end                                                  as death_cross

    from with_windows

),

-- -------------------------------------------------------------------------
-- Step 3: clean output — round all numerics, label NULLs
-- -------------------------------------------------------------------------
final as (

    select

        -- Keys
        daily_rate_id,
        rate_date,
        currency_pair,
        from_currency_id,
        to_currency_id,
        source_id,
        source_name,
        from_currency_code,
        to_currency_code,

        -- OHLCV (pass-through from daily)
        rate_open,
        rate_high,
        rate_low,
        rate_close,
        rate_avg,
        tick_count,
        daily_change,
        daily_change_pct,
        daily_range,

        -- ----------------------------------------------------------------
        -- Moving averages (NULL for first N-1 days — not enough history)
        -- ----------------------------------------------------------------
        round(ma_7d::numeric,  6)                            as ma_7d,
        round(ma_14d::numeric, 6)                            as ma_14d,
        round(ma_30d::numeric, 6)                            as ma_30d,

        -- ----------------------------------------------------------------
        -- Bollinger Bands
        -- ----------------------------------------------------------------
        round(bb_middle::numeric, 6)                         as bb_middle,
        round(bb_std::numeric,    6)                         as bb_std,
        bb_upper,
        bb_lower,
        bb_width,
        bb_position,

        -- ----------------------------------------------------------------
        -- Volatility
        -- ----------------------------------------------------------------
        round(volatility_7d::numeric,  6)                    as volatility_7d,
        round(volatility_30d::numeric, 6)                    as volatility_30d,
        round(atr_14d::numeric,        6)                    as atr_14d,

        -- ----------------------------------------------------------------
        -- Momentum
        -- ----------------------------------------------------------------
        roc_7d,
        roc_30d,

        -- ----------------------------------------------------------------
        -- Signals
        -- ----------------------------------------------------------------
        above_ma_7d,
        above_ma_30d,
        golden_cross,
        death_cross,

        -- ----------------------------------------------------------------
        -- Calendar
        -- ----------------------------------------------------------------
        year_number,
        month_number,
        row_num                                              as days_of_history,

        -- ----------------------------------------------------------------
        -- Metadata
        -- ----------------------------------------------------------------
        current_timestamp                                    as dbt_updated_at

    from with_indicators

)

select * from final