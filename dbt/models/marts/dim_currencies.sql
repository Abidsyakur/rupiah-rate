/*
================================================================================
dim_currencies.sql
================================================================================
Layer       : Marts (Dimension)
Upstream    : {{ ref('stg_currencies') }}
Materialized: table
Description :
    Business-ready currency dimension. One row per active currency.
    Adds classification columns useful for dashboard filtering/grouping.

Grain       : One row per currency_id
Used by     : fct_exchange_rates, fct_daily_snapshots (as from/to dimension)
================================================================================
*/

with

currencies as (

    select
        currency_id,
        currency_code,
        currency_name,
        currency_label,
        is_active,
        created_at,
        updated_at

    from {{ ref('stg_currencies') }}

),

final as (

    select

        -- ----------------------------------------------------------------
        -- Primary key
        -- ----------------------------------------------------------------
        currency_id,

        -- ----------------------------------------------------------------
        -- Business attributes
        -- ----------------------------------------------------------------
        currency_code,
        currency_name,
        currency_label,
        is_active,

        -- ----------------------------------------------------------------
        -- Classification — supports dashboard filters/grouping
        -- ----------------------------------------------------------------

        /*
        Tracked pairs in this platform are quoted against IDR. We flag IDR
        itself as the "base" / quote currency and everything else as a
        "trading partner" currency.
        */
        case
            when currency_code = 'IDR' then 'BASE'
            else                            'TRADING_PARTNER'
        end                                             as currency_role,

        /*
        Coarse region grouping for dashboard slicers. Maintained as a
        simple CASE for the currently-tracked currency set; extend as new
        pairs are added.
        */
        case currency_code
            when 'USD' then 'North America'
            when 'EUR' then 'Europe'
            when 'GBP' then 'Europe'
            when 'JPY' then 'Asia Pacific'
            when 'SGD' then 'Asia Pacific'
            when 'AUD' then 'Asia Pacific'
            when 'IDR' then 'Asia Pacific'
            else            'Other'
        end                                             as region,

        -- ----------------------------------------------------------------
        -- Audit
        -- ----------------------------------------------------------------
        created_at,
        updated_at,
        current_timestamp                               as dbt_updated_at

    from currencies

)

select * from final