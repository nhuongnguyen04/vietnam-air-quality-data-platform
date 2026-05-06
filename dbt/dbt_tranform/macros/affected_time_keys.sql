{% macro affected_hours_cte(source_relation, timestamp_column='datetime_hour', sync_run_column='raw_sync_run_id', fallback_column='raw_loaded_at') %}
affected_hours as (
    select distinct {{ timestamp_column }} as affected_hour
    from {{ source_relation }}
    where {{ downstream_incremental_predicate(sync_run_column, fallback_column) }}
)
{% endmacro %}

{% macro affected_dates_cte(source_relation, date_column='date', sync_run_column='raw_sync_run_id', fallback_column='raw_loaded_at') %}
affected_dates as (
    select distinct {{ date_column }} as affected_date
    from {{ source_relation }}
    where {{ downstream_incremental_predicate(sync_run_column, fallback_column) }}
)
{% endmacro %}

{% macro affected_months_cte(source_relation, month_column='month', sync_run_column='raw_sync_run_id', fallback_column='raw_loaded_at') %}
affected_months as (
    select distinct {{ month_column }} as affected_month
    from {{ source_relation }}
    where {{ downstream_incremental_predicate(sync_run_column, fallback_column) }}
)
{% endmacro %}
