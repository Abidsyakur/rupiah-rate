/*
================================================================================
stg_api_calls.sql
================================================================================
Layer       : Staging
Source      : {{ source('raw', 'api_calls') }}
Materialized: view
Description :
    Cleans and enriches the api_calls audit trail table.

    The api_calls table is INSERT-ONLY (no updated_at) — every ETL run
    appends a new row regardless of outcome. This staging model:

    - Standardises status to uppercase
    - Derives success/failure boolean flag from status
    - Calculates records_invalid when not explicitly stored
    - Derives execution_time_seconds from execution_time_ms
    - Extracts date/hour from call timestamp for trend analysis
    - Classifies latency into performance tiers
    - Adds is_recent flag (last 24h) for operational dashboards

Downstream  : int_api_call_summary.sql, int_pipeline_health.sql
Tests       : unique(call_id), not_null(call_id, source_id, status, created_at)
              accepted_values(status: SUCCESS, TIMEOUT, RATE_LIMIT, ERROR)
================================================================================
*/

with

source as (

    select
        call_id,
        source_id,
        timestamp,
        status,
        error_message,
        records_fetched,
        records_valid,
        records_invalid,
        execution_time_ms,
        created_at

    from {{ source('raw', 'api_calls') }}

),

cleaned as (

    select

        -- ----------------------------------------------------------------
        -- Primary key
        -- ----------------------------------------------------------------
        call_id,

        -- ----------------------------------------------------------------
        -- Foreign keys
        -- ----------------------------------------------------------------
        source_id,

        -- ----------------------------------------------------------------
        -- Core columns — standardised
        -- ----------------------------------------------------------------

        -- Timestamp when the API call was initiated (UTC)
        timestamp::timestamptz                              as call_timestamp,

        -- Status normalised to uppercase to guard against any case variance
        upper(trim(status::text))                                 as status,

        -- Error message cleaned of leading/trailing whitespace; NULL if none
        nullif(trim(error_message), '')                     as error_message,

        -- ----------------------------------------------------------------
        -- Record counts
        -- ----------------------------------------------------------------

        -- Total records the API reported back (may be NULL if call failed)
        coalesce(records_fetched, 0)                        as records_fetched,

        -- Records that passed validation (NULL treated as 0)
        coalesce(records_valid, 0)                          as records_valid,

        /*
        Records that failed validation.
        Prefer the stored value; fall back to derived value if NULL:
            records_invalid = records_fetched - records_valid
        Clamped to 0 to handle edge cases where stored counts are slightly
        inconsistent (e.g. due to mid-flight retries).
        */
        coalesce(
            records_invalid,
            greatest(
                coalesce(records_fetched, 0) - coalesce(records_valid, 0),
                0
            )
        )                                                   as records_invalid,

        -- ----------------------------------------------------------------
        -- Timing
        -- ----------------------------------------------------------------

        -- Raw milliseconds (may be NULL for calls that errored before timing)
        execution_time_ms,

        -- Human-friendly seconds with 3 decimal places
        round(execution_time_ms / 1000.0, 3)                as execution_time_seconds,

        -- ----------------------------------------------------------------
        -- Derived boolean flags
        -- ----------------------------------------------------------------

        /*
        Simple success flag — TRUE only for STATUS = 'SUCCESS'.
        PARTIAL_SUCCESS is not a valid status in this schema; a call
        with some failed records still lands as SUCCESS here if the API
        itself responded normally.
        */
        case
            when upper(trim(status::text)) = 'SUCCESS' then true
            else false
        end                                                 as is_success,

        /*
        True if the call hit a rate limit (useful for monitoring dashboards
        to detect over-aggressive scheduling).
        */
        case
            when upper(trim(status::text)) = 'RATE_LIMIT' then true
            else false
        end                                                 as is_rate_limited,

        /*
        True if the call timed out — correlate with source latency trends.
        */
        case
            when upper(trim(status::text)) = 'TIMEOUT' then true
            else false
        end                                                 as is_timeout,

        -- ----------------------------------------------------------------
        -- Latency tier classification
        -- ----------------------------------------------------------------

        /*
        Classifies API call performance into four tiers based on
        execution_time_ms. Thresholds are tuned for typical yfinance
        and FRED response times:
          FAST    < 500ms   — healthy, expected for most extractions
          NORMAL  500ms – 2s
          SLOW    2s – 10s  — worth monitoring; check for source issues
          TIMEOUT > 10s or NULL (NULL = call died before timing was recorded)
        */
        case
            when execution_time_ms is null              then 'UNKNOWN'
            when execution_time_ms < 500                then 'FAST'
            when execution_time_ms < 2000               then 'NORMAL'
            when execution_time_ms < 10000              then 'SLOW'
            else                                             'CRITICAL'
        end                                                 as latency_tier,

        -- ----------------------------------------------------------------
        -- Date / time decomposition
        -- ----------------------------------------------------------------

        -- Calendar date of the call (UTC)
        date(timestamp at time zone 'UTC')                  as call_date,

        -- Hour of day (0 – 23, UTC) — used for heatmap / intraday trending
        extract(hour from timestamp at time zone 'UTC')::int
                                                            as call_hour,

        -- Day of week (1 = Monday … 7 = Sunday)
        extract(isodow from timestamp at time zone 'UTC')::int
                                                            as day_of_week,

        -- Month and year for rollup reporting
        extract(month from timestamp at time zone 'UTC')::int
                                                            as month_number,
        extract(year from timestamp at time zone 'UTC')::int
                                                            as year_number,
        to_char(timestamp at time zone 'UTC', 'YYYY-MM')   as year_month,

        -- ----------------------------------------------------------------
        -- Operational flags
        -- ----------------------------------------------------------------

        /*
        is_recent: TRUE if the call happened within the last 24 hours.
        Used by operational dashboards and the Airflow health-check DAG to
        surface the most current extraction status without date filtering.
        */
        case
            when timestamp >= now() - interval '24 hours' then true
            else false
        end                                                 as is_recent,

        -- ----------------------------------------------------------------
        -- Audit timestamp (created_at only — insert-only table)
        -- ----------------------------------------------------------------
        created_at::timestamptz                             as created_at

    from source

)

/*
No additional filtering on api_calls — every call record (success OR
failure) is valuable for monitoring. Downstream intermediate models
filter by status when computing success rates and SLA metrics.
*/
select * from cleaned