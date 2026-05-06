{% macro downstream_incremental_predicate(
    sync_run_column='raw_sync_run_id',
    fallback_column='raw_loaded_at',
    lookback_hours=var('staging_incremental_lookback_hours', 6)
) %}
    {% set sync_run_id = var('staging_sync_run_id', '') %}
    {% if is_incremental() %}
        {% if sync_run_id %}
{{ sync_run_column }} = '{{ sync_run_id | replace("'", "''") }}'
        {% else %}
{{ fallback_column }} >= now() - interval {{ lookback_hours }} hour
        {% endif %}
    {% else %}
1 = 1
    {% endif %}
{% endmacro %}
