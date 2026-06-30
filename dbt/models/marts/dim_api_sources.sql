/*
================================================================================
dim_api_sources.sql
================================================================================
Layer       : Marts (Dimension)
Upstream    : {{ ref('stg_api_sources') }}
             {{ ref('int_api_performance') }}   (for current health snapshot)
Materialized: table
Description :
    Business-ready API source dimension, enriched with a rolling 7-day
    health snapshot so dashboards can show source reliability alongside
    its configuration without a separate join to the performance fact table.

Grain       : One row per source_id
Used by     : fct_exchange_rates (source dimension), monitoring dashboards
================================================================================
*/

with

sources as (

    select
        source_id,
        source_name,
        source_name_display,
        api_endpoint,
        retry_strategy,
        rate_limit,
        rate_limit_tier,
        source_category,
        is_active,
        created_at,
        updated_at

    from {{ ref('stg_api_sources') }}

),

-- -------------------------------------------------------------------------
-- Rolling 7-day health snapshot (most recent 7 days of int_api_performance)
-- -------------------------------------------------------------------------
recent_performance as (

    select
        source_id,
        sum(total_calls)                                    as calls_last_7d,
        sum(success_calls)                                  as success_calls_last_7d,
        sum(failed_calls)                                   as failed_calls_last_7d,
        round(avg(success_rate_pct), 2)                     as avg_success_rate_7d,
        round(avg(avg_execution_ms), 0)                     as avg_latency_ms_7d,
        max(p95_execution_ms)                               as worst_p95_latency_7d,
        bool_or(sla_breached)                               as had_sla_breach_7d,
        max(call_date)                                      as last_call_date

    from {{ ref('int_api_performance') }}
    where call_date >= current_date - interval '7 days'
    group by source_id

),

final as (

    select

        -- ----------------------------------------------------------------
        -- Primary key
        -- ----------------------------------------------------------------
        s.source_id,

        -- ----------------------------------------------------------------
        -- Business attributes
        -- ----------------------------------------------------------------
        s.source_name,
        s.source_name_display,
        s.api_endpoint,
        s.retry_strategy,
        s.rate_limit,
        s.rate_limit_tier,
        s.source_category,
        s.is_active,

        -- ----------------------------------------------------------------
        -- Rolling 7-day health snapshot
        -- ----------------------------------------------------------------
        coalesce(p.calls_last_7d, 0)                       as calls_last_7d,
        coalesce(p.success_calls_last_7d, 0)               as success_calls_last_7d,
        coalesce(p.failed_calls_last_7d, 0)                as failed_calls_last_7d,
        p.avg_success_rate_7d,
        p.avg_latency_ms_7d,
        p.worst_p95_latency_7d,
        coalesce(p.had_sla_breach_7d, false)               as had_sla_breach_7d,
        p.last_call_date,

        -- ----------------------------------------------------------------
        -- Derived health status — used for dashboard status indicators
        -- ----------------------------------------------------------------

        /*
        Overall health classification combining recency and reliability:
          HEALTHY      No calls in the last 7 days that breached SLA,
                       and at least one call was made recently.
          DEGRADED     SLA was breached at least once in the last 7 days
                       but the source has made calls recently.
          NO_DATA      No calls recorded in the last 7 days at all
                       (possible scheduling gap or disabled extraction).
          INACTIVE     Source itself is marked is_active = false.
        */
        case
            when s.is_active = false                          then 'INACTIVE'
            when p.calls_last_7d is null or p.calls_last_7d = 0
                                                              then 'NO_DATA'
            when p.had_sla_breach_7d = true                   then 'DEGRADED'
            else                                                    'HEALTHY'
        end                                                 as health_status,

        -- Days since the last successful call (for staleness alerts)
        case
            when p.last_call_date is null then null
            else current_date - p.last_call_date
        end                                                 as days_since_last_call,

        -- ----------------------------------------------------------------
        -- Audit
        -- ----------------------------------------------------------------
        s.created_at,
        s.updated_at,
        current_timestamp                                   as dbt_updated_at

    from sources              s
    left join recent_performance p
        on s.source_id = p.source_id

)

select * from final