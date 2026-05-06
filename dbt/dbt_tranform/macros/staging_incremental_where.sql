{% macro staging_incremental_where(sync_run_column='raw_sync_run_id', fallback_column='raw_loaded_at') %}
    {% if is_incremental() %}
        {% set sync_run_id = var('staging_sync_run_id', '') %}
        {% if sync_run_id %}
    where {{ sync_run_column }} = '{{ sync_run_id | replace("'", "''") }}'
        {% else %}
    where {{ fallback_column }} >= now() - interval {{ var('staging_incremental_lookback_hours', 6) }} hour
        {% endif %}
    {% endif %}
{% endmacro %}
