/*
================================================================================
stg_currencies.sql
================================================================================
Layer       : Staging
Source      : {{ source('raw', 'currencies') }}
Materialized: view
Description :
    Cleans and standardises the currencies dimension table.
    - Filters out soft-deleted currencies (is_active = false)
    - Uppercases ISO 4217 code for consistency
    - Adds a surrogate display label (code + name)
    - Casts timestamps to UTC for downstream consistency

Downstream  : int_exchange_rates_enriched.sql
Tests       : unique(currency_id), unique(currency_code), not_null(currency_id)
================================================================================
*/

with

source as (

    /*
    Pull all columns from the raw currencies table.
    No filtering here — raw CTE is always unfiltered so dbt lineage
    and source freshness tests can reference the full row count.
    */
    select
        currency_id,
        code,
        name,
        is_active,
        created_at,
        updated_at

    from {{ source('raw', 'currencies') }}

),

cleaned as (

    select

        -- ----------------------------------------------------------------
        -- Primary key
        -- ----------------------------------------------------------------
        currency_id,

        -- ----------------------------------------------------------------
        -- Business columns — standardised
        -- ----------------------------------------------------------------

        -- Enforce uppercase ISO 4217 code regardless of how it was stored
        upper(trim(code))                               as currency_code,

        -- Trim whitespace from display name
        trim(name)                                      as currency_name,

        -- Boolean flag kept as-is; renamed for clarity
        is_active,

        -- ----------------------------------------------------------------
        -- Derived / convenience columns
        -- ----------------------------------------------------------------

        -- Human-readable label for reports (e.g. "USD – US Dollar")
        upper(trim(code)) || ' – ' || trim(name)        as currency_label,

        -- ----------------------------------------------------------------
        -- Audit timestamps (cast to timestamptz for cross-dialect safety)
        -- ----------------------------------------------------------------
        created_at::timestamptz                         as created_at,
        updated_at::timestamptz                         as updated_at

    from source

),

filtered as (

    /*
    Staging filter: only pass active currencies downstream.
    Soft-deleted rows are excluded so intermediate and mart models never
    need to repeat this filter themselves.

    To include inactive rows for an audit query, query the source directly:
        select * from {{ source('raw', 'currencies') }} where is_active = false
    */
    select *
    from cleaned
    where is_active = true

)

select * from filtered