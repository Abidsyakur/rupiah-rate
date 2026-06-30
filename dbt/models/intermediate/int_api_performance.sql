/*
================================================================================
int_api_performance.sql
================================================================================
Layer       : Intermediate
Upstream    : {{ ref('stg_api_calls') }}
             {{ ref('stg_api_sources') }}
Materialized: table
Description :
    Aggregates API call audit records into performance and reliability metrics
    per source, per day, and per hour. Used by the monitoring dashboard and
    the Airflow health-check DAG to detect degradation early.
================================================================================
*/

with

calls as (

    select
        call_id,
        source_id,
        -- ✂️ Hapus source_name dari sini karena tidak ada di stg_api_calls
        call_timestamp,
        call_date,
        call_hour,
        day_of_week,
        month_number,
        year_number,
        year_month,
        status,
        is_success,
        is_rate_limited,
        is_timeout,
        error_message,
        records_fetched,
        records_valid,
        records_invalid,
        execution_time_ms,
        execution_time_seconds,
        latency_tier,
        is_recent

    from {{ ref('stg_api_calls') }}

),

sources as (

    select
        source_id,
        source_name,
        source_category,
        rate_limit,
        rate_limit_tier

    from {{ ref('stg_api_sources') }}

),

-- -------------------------------------------------------------------------
-- Step 1: hourly aggregation per source
-- -------------------------------------------------------------------------
hourly_agg as (

    select

        c.source_id,
        s.source_name, -- 👈 UBAH MENJADI s.source_name (diambil dari CTE sources)
        c.call_date,
        c.call_hour,
        c.day_of_week,
        c.month_number,
        c.year_number,
        c.year_month,

        -- ----------------------------------------------------------------
        -- Volume metrics
        -- ----------------------------------------------------------------
        count(*)                                                as total_calls,

        count(*) filter (where c.is_success = true)             as success_calls,

        count(*) filter (
            where c.status = 'ERROR'
        )                                                       as failed_calls,

        count(*) filter (where c.is_timeout = true)             as timeout_calls,

        count(*) filter (where c.is_rate_limited = true)        as rate_limited_calls,

        -- ----------------------------------------------------------------
        -- Throughput metrics
        -- ----------------------------------------------------------------
        sum(c.records_fetched)                                  as total_records_fetched,
        sum(c.records_valid)                                    as total_records_valid,
        sum(c.records_invalid)                                  as total_records_invalid,

        -- ----------------------------------------------------------------
        -- Latency metrics
        -- ----------------------------------------------------------------

        avg(c.execution_time_ms)                                as avg_execution_ms,

        -- Median (p50) — approximated with percentile_cont
        percentile_cont(0.50) within group (
            order by c.execution_time_ms
        )                                                       as p50_execution_ms,

        percentile_cont(0.95) within group (
            order by c.execution_time_ms
        )                                                       as p95_execution_ms,

        max(c.execution_time_ms)                                as max_execution_ms,
        min(c.execution_time_ms)                                as min_execution_ms,

        -- ----------------------------------------------------------------
        -- Latency tier distribution
        -- ----------------------------------------------------------------
        count(*) filter (
            where c.latency_tier = 'FAST'
        )                                                       as fast_calls,

        count(*) filter (
            where c.latency_tier = 'NORMAL'
        )                                                       as normal_calls,

        count(*) filter (
            where c.latency_tier = 'SLOW'
        )                                                       as slow_calls,

        count(*) filter (
            where c.latency_tier = 'CRITICAL'
        )                                                       as critical_latency_calls,

        -- ----------------------------------------------------------------
        -- Recent flag: is at least one call in this window recent?
        -- ----------------------------------------------------------------
        bool_or(c.is_recent)                                    as has_recent_calls,

        -- Most common error message in this window (for diagnostics)
        mode() within group (
            order by c.error_message
        ) filter (where c.error_message is not null)            as most_common_error

    from calls    c
    inner join sources s using (source_id)
    group by
        c.source_id,
        s.source_name, -- 👈 UBAH MENJADI s.source_name agar sesuai dengan SELECT di atas
        c.call_date,
        c.call_hour,
        c.day_of_week,
        c.month_number,
        c.year_number,
        c.year_month

),

-- -------------------------------------------------------------------------
-- Step 2: derive rates, yields, and SLA flags
-- -------------------------------------------------------------------------
with_rates as (

    select

        *,

        -- ----------------------------------------------------------------
        -- Reliability rates
        -- ----------------------------------------------------------------

        case
            when total_calls = 0 then null
            else round(success_calls::numeric / total_calls * 100, 2)
        end                                                     as success_rate_pct,

        case
            when total_calls = 0 then null
            else round(
                (failed_calls + timeout_calls)::numeric / total_calls * 100,
                2
            )
        end                                                     as error_rate_pct,

        /*
        Availability excludes rate-limit errors (those are scheduling
        issues, not source availability issues) and counts only hard
        failures (ERROR + TIMEOUT).
        */
        case
            when total_calls = 0 then null
            else round(
                100.0 - (
                    (failed_calls + timeout_calls)::numeric / total_calls * 100
                ),
                2
            )
        end                                                     as availability_pct,

        -- ----------------------------------------------------------------
        -- Throughput rates
        -- ----------------------------------------------------------------

        case
            when total_calls = 0 then null
            else round(total_records_fetched::numeric / total_calls, 2)
        end                                                     as avg_records_per_call,

        case
            when total_records_fetched = 0 or total_records_fetched is null then null
            else round(
                total_records_valid::numeric / total_records_fetched * 100,
                2
            )
        end                                                     as data_yield_pct,

        -- ----------------------------------------------------------------
        -- SLA breach flags
        -- ----------------------------------------------------------------

        /*
        SLA breach: success rate falls below the configured target.
        Default threshold is 95%. Override at runtime:
            dbt run --vars '{"sla_success_rate_target": 99}'
        */
        case
            when total_calls = 0 then false
            when round(success_calls::numeric / total_calls * 100, 2)
                 < {{ var('sla_success_rate_target', 95) }}
            then true
            else false
        end                                                     as sla_breached,

        /*
        Critical latency: p95 response time exceeds 10 seconds.
        Indicates the source is intermittently timing out or very slow.
        */
        case
            when p95_execution_ms is null    then false
            when p95_execution_ms > 10000    then true
            else false
        end                                                     as has_critical_latency,

        -- Rounded latency columns for cleaner reporting
        round(avg_execution_ms::numeric, 0)                     as avg_execution_ms_rounded,
        round(p50_execution_ms::numeric, 0)                     as p50_execution_ms_rounded,
        round(p95_execution_ms::numeric, 0)                     as p95_execution_ms_rounded

    from hourly_agg

),

-- -------------------------------------------------------------------------
-- Step 3: join source metadata and produce final columns
-- -------------------------------------------------------------------------
final as (

    select

        -- ----------------------------------------------------------------
        -- Surrogate key
        -- ----------------------------------------------------------------
      {{
            dbt_utils.generate_surrogate_key([
                'r.source_id',
                'call_date',
                'call_hour'
            ])
        }}                                                      as perf_id,

        -- ----------------------------------------------------------------
        -- Dimensions
        -- ----------------------------------------------------------------
        r.source_id,
        r.source_name, -- 
        s.source_category,
        s.rate_limit,
        s.rate_limit_tier,
        call_date,
        call_hour,
        day_of_week,
        month_number,
        year_number,
        year_month,

        -- Human-readable time window label (e.g. "2025-01-15 10:00")
        call_date::text || ' '
            || lpad(call_hour::text, 2, '0') || ':00'          as window_label,

        -- ----------------------------------------------------------------
        -- Volume
        -- ----------------------------------------------------------------
        total_calls,
        success_calls,
        failed_calls,
        timeout_calls,
        rate_limited_calls,
        fast_calls,
        normal_calls,
        slow_calls,
        critical_latency_calls,

        -- ----------------------------------------------------------------
        -- Reliability
        -- ----------------------------------------------------------------
        success_rate_pct,
        error_rate_pct,
        availability_pct,
        sla_breached,

        -- ----------------------------------------------------------------
        -- Throughput
        -- ----------------------------------------------------------------
        total_records_fetched,
        total_records_valid,
        total_records_invalid,
        avg_records_per_call,
        data_yield_pct,

        -- ----------------------------------------------------------------
        -- Latency
        -- ----------------------------------------------------------------
        avg_execution_ms_rounded                                as avg_execution_ms,
        p50_execution_ms_rounded                                as p50_execution_ms,
        p95_execution_ms_rounded                                as p95_execution_ms,
        max_execution_ms,
        min_execution_ms,
        has_critical_latency,

        -- ----------------------------------------------------------------
        -- Diagnostics
        -- ----------------------------------------------------------------
        most_common_error,
        has_recent_calls,

        -- ----------------------------------------------------------------
        -- Metadata
        -- ----------------------------------------------------------------
        current_timestamp                                       as dbt_updated_at

    from with_rates r
    inner join sources s on r.source_id = s.source_id -- 
)

select * from final