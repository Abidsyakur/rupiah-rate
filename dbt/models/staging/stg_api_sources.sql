/*
================================================================================
stg_api_sources.sql
================================================================================
Layer       : Staging
Source      : {{ source('raw', 'api_sources') }}
Materialized: view
Description :
    Cleans and standardises the api_sources configuration table.
    - Filters out disabled sources (is_active = false)
    - Lowercases source_name for consistent joining downstream
    - Derives a source_category column (official_api vs library)
    - Casts timestamps to UTC

Downstream  : int_exchange_rates_enriched.sql, int_api_call_summary.sql
Tests       : unique(source_id), unique(source_name), not_null(source_id)
================================================================================
*/

with

source as (

    /*
    Raw pull — unfiltered. Never add WHERE clauses here so the source
    CTE always represents the full raw table for lineage tracking.
    */
    select
        source_id,
        source_name,
        api_endpoint,
        retry_strategy,
        rate_limit,
        is_active,
        created_at,
        updated_at

    from {{ source('raw', 'api_sources') }}

),

cleaned as (

    select

        -- ----------------------------------------------------------------
        -- Primary key
        -- ----------------------------------------------------------------
        source_id,

        -- ----------------------------------------------------------------
        -- Business columns — standardised
        -- ----------------------------------------------------------------

        -- Lowercase for consistent joins (source_name is the natural key)
        lower(trim(source_name))                        as source_name,

        -- Original casing preserved for display purposes
        trim(source_name)                               as source_name_display,

        -- Endpoint URL stripped of trailing slash
        rtrim(api_endpoint, '/')                        as api_endpoint,

        -- Retry strategy — lowercase for programmatic use
        lower(retry_strategy)                           as retry_strategy,

        -- Requests per hour; NULL means undocumented / unlimited
        rate_limit,

        is_active,

        -- ----------------------------------------------------------------
        -- Derived columns
        -- ----------------------------------------------------------------

        /*
        Classify the source by how it is accessed:
          'library'      — accessed via a pip-installed Python library
                           (e.g. yfinance uses the yfinance package)
          'rest_api'     — accessed directly via HTTP REST with an API key
                           (e.g. FRED uses requests + FRED_API_KEY)
          'unknown'      — source not yet classified
        */
        case lower(trim(source_name))
            when 'yfinance'     then 'library'
            when 'fred'         then 'rest_api'
            else                     'unknown'
        end                                             as source_category,

        /*
        Rate-limit tier to help downstream alerting models decide
        how aggressively to schedule extractions:
          'high'    > 1000 req/h
          'medium'  101 – 1000 req/h
          'low'     1 – 100 req/h
          'unknown' NULL rate_limit
        */
        case
            when rate_limit is null         then 'unknown'
            when rate_limit > 1000          then 'high'
            when rate_limit between 101
                                 and 1000   then 'medium'
            else                                 'low'
        end                                             as rate_limit_tier,

        -- ----------------------------------------------------------------
        -- Audit timestamps
        -- ----------------------------------------------------------------
        created_at::timestamptz                         as created_at,
        updated_at::timestamptz                         as updated_at

    from source

),

filtered as (

    /*
    Only active sources are passed downstream. Inactive sources remain
    in the raw table for FK integrity but should not appear in any
    mart or report.
    */
    select *
    from cleaned
    where is_active = true

)

select * from filtered