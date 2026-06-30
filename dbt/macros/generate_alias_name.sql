{#
================================================================================
generate_alias_name.sql
================================================================================
Macro       : generate_alias_name
Type        : dbt macro override (built-in hook)
Description :
    Overrides dbt's default `generate_alias_name` macro to control how
    model names map to physical table/view names in the database.

    Default dbt behaviour: alias = model file name (e.g. stg_currencies.sql
    becomes table "stg_currencies"). This override preserves that default
    but adds two project-specific conventions:

    1. Strips the layer prefix (stg_/int_/fct_/dim_) when an explicit
       `alias` config is NOT set AND the model lives in a layer where we
       want shorter physical names for BI tool convenience — controlled
       via the `simplify_mart_aliases` var (default: false, i.e. off).

    2. Always lower-cases the final alias, since Postgres treats
       unquoted identifiers as case-insensitive but quoted ones as
       case-sensitive — staying consistently lowercase avoids subtle
       cross-tool bugs (e.g. a BI tool quoting "Fct_Daily_Snapshots").

    Usage:
        This macro is called automatically by dbt for every model — no
        explicit invocation needed. To opt a mart model into the shortened
        alias behaviour, set the var in dbt_project.yml or via --vars:

            dbt run --vars '{"simplify_mart_aliases": true}'

        With simplify_mart_aliases=true:
            fct_daily_snapshots  ->  daily_snapshots
            dim_currencies       ->  currencies

        With simplify_mart_aliases=false (default):
            fct_daily_snapshots  ->  fct_daily_snapshots  (unchanged)
================================================================================
#}

{% macro generate_alias_name(custom_alias_name=none, node=none) %}

    {#- Respect an explicit alias: config(alias='...') always wins -#}
    {%- if custom_alias_name is not none -%}

        {{ custom_alias_name | trim | lower }}

    {%- else -%}

        {%- set raw_name = node.name -%}
        {%- set simplify = var('simplify_mart_aliases', false) -%}
        {%- set known_prefixes = ['stg_', 'int_', 'fct_', 'dim_'] -%}

        {%- set final_name = raw_name -%}

        {%- if simplify -%}
            {%- for prefix in known_prefixes -%}
                {%- if raw_name.startswith(prefix) -%}
                    {%- set final_name = raw_name[(prefix | length):] -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endif -%}

        {{ final_name | trim | lower }}

    {%- endif -%}

{% endmacro %}