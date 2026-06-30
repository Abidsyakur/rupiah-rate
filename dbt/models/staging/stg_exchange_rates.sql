/*
================================================================================
stg_exchange_rates.sql
================================================================================
Layer       : Staging
Source      : {{ source('raw', 'exchange_rates') }}
Materialized: view
Description :
    Cleans, filters, and enriches raw exchange rate observations before
    the intermediate layer joins them with dimension tables.

    Transformations applied:
    - Removes records flagged is_valid = false by the validation layer
    - Casts rate to NUMERIC for precision-safe downstream arithmetic
    - Extracts date, hour, day_of_week, week, month, year from timestamp
    - Derives rate_precision (number of significant decimal places)
    - Derives quality_band (HIGH / MEDIUM / LOW / UNSCORED) from quality score
    - Adds is_stale flag: records older than {{ var('freshness_threshold_hours') }}h
    - Adds is_business_day flag based on ISO day-of-week

    Quality filter:
    - is_valid = true  (hard check from validators.py)
    - rate > 0         (belt-and-suspenders; also enforced by DB CHECK)

    To inspect excluded records, query the source directly:
        select * from {{ source('raw', 'exchange_rates') }} where is_valid = false

Downstream  : int_exchange_rates_enriched.sql
Variables   : freshness_threshold_hours (default 48), min_quality_score (default 0.0)
Tests       : unique(rate_id), not_null(rate_id, from_currency_id, to_currency_id,
              source_id, rate, timestamp)
================================================================================
*/

with

source as (

    select
        rate_id,
        from_currency_id,
        to_currency_id,
        source_id,
        rate,
        timestamp,
        data_quality_score,
        is_valid,
        created_at,
        updated_at

    from {{ source('raw', 'exchange_rates') }}

),

cleaned as (

    select

        -- ----------------------------------------------------------------
        -- Primary key
        -- ----------------------------------------------------------------
        rate_id,

        -- ----------------------------------------------------------------
        -- Foreign keys (unchanged — joins happen in intermediate layer)
        -- ----------------------------------------------------------------
        from_currency_id,
        to_currency_id,
        source_id,

        -- ----------------------------------------------------------------
        -- Core business columns
        -- ----------------------------------------------------------------

        -- Cast to NUMERIC for safe arithmetic; raw column is already
        -- NUMERIC(12,6) but explicit cast documents intent and protects
        -- against dialect changes.
        rate::numeric(20, 8)                            as rate,

        -- Market timestamp normalised to UTC
        timestamp::timestamptz                          as rate_timestamp,

        -- Quality score (0.00 – 1.00); NULL = not yet evaluated
        data_quality_score::numeric(3, 2)               as data_quality_score,

        -- Validation flag from validators.py
        is_valid,

        -- ----------------------------------------------------------------
        -- Date / time decomposition (for partitioning and grouping)
        -- ----------------------------------------------------------------

        -- Calendar date of the observation (UTC)
        date(timestamp at time zone 'UTC')              as rate_date,

        -- Hour of day (0 – 23, UTC) — useful for intraday analysis
        extract(hour from timestamp at time zone 'UTC')::int
                                                        as rate_hour,

        -- ISO day of week: 1 = Monday … 7 = Sunday
        extract(isodow from timestamp at time zone 'UTC')::int
                                                        as day_of_week,

        -- Calendar week number (ISO week)
        extract(week from timestamp at time zone 'UTC')::int
                                                        as week_of_year,

        -- Calendar month (1 – 12)
        extract(month from timestamp at time zone 'UTC')::int
                                                        as month_number,

        -- Calendar year
        extract(year from timestamp at time zone 'UTC')::int
                                                        as year_number,

        -- YYYY-MM string for easy monthly grouping
        to_char(timestamp at time zone 'UTC', 'YYYY-MM')
                                                        as year_month,

        -- ----------------------------------------------------------------
        -- Derived quality / freshness columns
        -- ----------------------------------------------------------------

        /*
        Quality band classification for reporting dashboards:
          HIGH    quality_score >= 0.9
          MEDIUM  quality_score >= 0.7 and < 0.9
          LOW     quality_score >= 0.0 and < 0.7
          UNSCORED quality_score is NULL
        Threshold aligns with PIPELINE_VALIDATE_QUALITY_THRESHOLD default (0.7).
        */
        case
            when data_quality_score is null         then 'UNSCORED'
            when data_quality_score >= 0.9          then 'HIGH'
            when data_quality_score >= 0.7          then 'MEDIUM'
            else                                         'LOW'
        end                                             as quality_band,

        /*
        Stale flag: TRUE if the observation is older than the configured
        freshness threshold. Useful for alerting models and dashboards that
        need to surface missing-data gaps.
        Uses 'var()' so the threshold can be overridden at runtime:
            dbt run --vars '{"freshness_threshold_hours": 4}'
        */
        case
            when timestamp < now() - interval '1 hour'
                             * {{ var('freshness_threshold_hours', 48) }}
            then true
            else false
        end                                             as is_stale,

        /*
        Business day flag (Monday–Friday = true) based on ISO day of week.
        Exchange rates on weekends typically repeat Friday's close in some
        sources — downstream models may want to exclude or handle these.
        */
        case
            when extract(isodow from timestamp at time zone 'UTC') <= 5
            then true
            else false
        end                                             as is_business_day,

        /*
        Number of significant decimal places in the rate value.
        Useful for detecting unusually low-precision data from certain
        sources (e.g. a rate stored as 16000.0 vs 16012.345678).
        Capped at 8 to match the NUMERIC(20,8) cast above.
        */
        length(
            split_part(
                trim(trailing '0' from rate::text),
                '.', 2
            )
        )                                               as rate_decimal_places,

        -- ----------------------------------------------------------------
        -- Audit timestamps
        -- ----------------------------------------------------------------
        created_at::timestamptz                         as created_at,
        updated_at::timestamptz                         as updated_at

    from source

),

filtered as (

    /*
    Hard filters — records that fail these checks are not fit for analytics:

    1. is_valid = true:  Rows rejected by ExchangeRateValidator (hard checks:
                         null rate, out-of-range, etc.) are excluded.
    2. rate > 0:         Belt-and-suspenders guard. The DB CHECK constraint
                         already blocks negatives at write time; this protects
                         against data loaded through non-standard paths.
    3. quality >= min:   Configurable floor via dbt var 'min_quality_score'
                         (default 0.0 = no floor, all scores pass).
                         Raise it in production to restrict to trusted data:
                             dbt run --vars '{"min_quality_score": 0.7}'
    */
    select *
    from cleaned
    where
        is_valid = true
        and rate > 0
        and (
            data_quality_score is null
            or data_quality_score >= {{ var('min_quality_score', 0.0) }}
        )

)

select * from filtered