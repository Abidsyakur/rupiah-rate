{#
================================================================================
custom_transformations.sql
================================================================================
Macro file containing reusable SQL-generating macros used across staging,
intermediate, and marts models. Centralising these avoids copy-pasted CASE
expressions drifting out of sync (e.g. quality_band thresholds being
defined slightly differently in three different models).
================================================================================
#}


{#
--------------------------------------------------------------------------
quality_band(quality_score_column)
--------------------------------------------------------------------------
Returns a SQL CASE expression classifying a quality score column into
HIGH / MEDIUM / LOW / UNSCORED bands.

Thresholds match PIPELINE_VALIDATE_QUALITY_THRESHOLD default (0.7) and
the bands already hand-coded in stg_exchange_rates.sql — this macro lets
future models reuse the exact same logic instead of re-deriving it.

Usage:
    select
        rate_id,
        {{ quality_band('data_quality_score') }} as quality_band
    from {{ ref('stg_exchange_rates') }}
--------------------------------------------------------------------------
#}
{% macro quality_band(quality_score_column) %}
    case
        when {{ quality_score_column }} is null      then 'UNSCORED'
        when {{ quality_score_column }} >= 0.9        then 'HIGH'
        when {{ quality_score_column }} >= 0.7        then 'MEDIUM'
        else                                                'LOW'
    end
{% endmacro %}


{#
--------------------------------------------------------------------------
pct_change(new_value_column, old_value_column, decimal_places=4)
--------------------------------------------------------------------------
Returns a SQL expression computing percentage change between two columns,
safely handling division by zero / NULL old_value.

    pct_change = (new_value - old_value) / old_value * 100

Usage:
    select
        rate_date,
        {{ pct_change('rate_close', 'rate_open') }} as daily_change_pct
    from {{ ref('int_daily_exchange_rates') }}
--------------------------------------------------------------------------
#}
{% macro pct_change(new_value_column, old_value_column, decimal_places=4) %}
    case
        when {{ old_value_column }} is null or {{ old_value_column }} = 0 then null
        else round(
            ({{ new_value_column }} - {{ old_value_column }})
            / {{ old_value_column }} * 100,
            {{ decimal_places }}
        )
    end
{% endmacro %}


{#
--------------------------------------------------------------------------
safe_divide(numerator_column, denominator_column, decimal_places=4)
--------------------------------------------------------------------------
Returns a NULL-safe, zero-safe division expression. Used throughout the
intermediate and marts layers anywhere a rate or percentage is derived
from a count that could legitimately be zero (e.g. pass_rate when
total_rates_evaluated = 0 on a day with no data).

Usage:
    select
        {{ safe_divide('rates_all_passed', 'total_rates_evaluated', 2) }}
            as pass_rate_fraction
    from {{ ref('int_quality_summary') }}
--------------------------------------------------------------------------
#}
{% macro safe_divide(numerator_column, denominator_column, decimal_places=4) %}
    case
        when {{ denominator_column }} is null or {{ denominator_column }} = 0 then null
        else round(
            {{ numerator_column }}::numeric / {{ denominator_column }},
            {{ decimal_places }}
        )
    end
{% endmacro %}


{#
--------------------------------------------------------------------------
utc_date_parts(timestamp_column)
--------------------------------------------------------------------------
Returns a comma-separated list of standard calendar columns derived from
a timestamp column, all normalised to UTC. Designed to be dropped directly
into a SELECT list to avoid retyping the same six EXTRACT() expressions
in every staging model that has a timestamp.

NOTE: Because this macro emits multiple comma-separated columns, it must
be the LAST item in the SELECT list (or followed by more columns with a
leading comma added manually) — see usage example.

Usage:
    select
        rate_id,
        rate,
        {{ utc_date_parts('timestamp') }}
    from {{ source('raw', 'exchange_rates') }}

Emits columns named:
    <alias>_date, <alias>_hour, day_of_week, week_of_year,
    month_number, year_number, year_month
where <alias> defaults to 'rate' — pass a second arg to override:
    {{ utc_date_parts('call_timestamp', 'call') }}
--------------------------------------------------------------------------
#}
{% macro utc_date_parts(timestamp_column, alias='rate') %}
    date({{ timestamp_column }} at time zone 'UTC')                  as {{ alias }}_date,
    extract(hour from {{ timestamp_column }} at time zone 'UTC')::int
                                                                      as {{ alias }}_hour,
    extract(isodow from {{ timestamp_column }} at time zone 'UTC')::int
                                                                      as day_of_week,
    extract(week from {{ timestamp_column }} at time zone 'UTC')::int
                                                                      as week_of_year,
    extract(month from {{ timestamp_column }} at time zone 'UTC')::int
                                                                      as month_number,
    extract(year from {{ timestamp_column }} at time zone 'UTC')::int
                                                                      as year_number,
    to_char({{ timestamp_column }} at time zone 'UTC', 'YYYY-MM')
                                                                      as year_month
{% endmacro %}


{#
--------------------------------------------------------------------------
traffic_light_flag(red_condition, yellow_condition)
--------------------------------------------------------------------------
Returns a standard RED / YELLOW / GREEN CASE expression. Used to keep
the quality_flag logic in fct_exchange_rates.sql and
fct_daily_snapshots.sql expressible as a single reusable pattern, even
though the actual breach/degraded conditions differ per model (hence
they are passed in as raw SQL fragments rather than hardcoded).

Usage:
    select
        rate_id,
        {{ traffic_light_flag(
            "is_stale = true or daily_quality_sla_breached = true",
            "quality_band in ('LOW', 'MEDIUM')"
        ) }} as quality_flag
    from ...

WARNING: the condition arguments are interpolated directly as raw SQL.
Never pass user-supplied input to this macro — only use it with static,
developer-authored condition strings.
--------------------------------------------------------------------------
#}
{% macro traffic_light_flag(red_condition, yellow_condition) %}
    case
        when {{ red_condition }}     then 'RED'
        when {{ yellow_condition }}  then 'YELLOW'
        else                              'GREEN'
    end
{% endmacro %}


{#
--------------------------------------------------------------------------
currency_pair_string(from_code_column, to_code_column)
--------------------------------------------------------------------------
Returns the canonical "FROM_TO" pair string expression (e.g. "USD_IDR"),
matching the convention used throughout src/etl/extractors.py.

Usage:
    select
        {{ currency_pair_string('fc.currency_code', 'tc.currency_code') }}
            as currency_pair
    from ...
--------------------------------------------------------------------------
#}
{% macro currency_pair_string(from_code_column, to_code_column) %}
    {{ from_code_column }} || '_' || {{ to_code_column }}
{% endmacro %}