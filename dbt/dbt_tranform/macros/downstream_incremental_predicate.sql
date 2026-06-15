{% macro downstream_incremental_predicate(
    sync_run_column='raw_sync_run_id',
    fallback_column='raw_loaded_at'
) %}
    {% if is_incremental() %}
        {{ fallback_column }} > (
            select coalesce(max(last_success), toDateTime('1970-01-01 00:00:00')) 
            from {{ source('core_external', 'ingestion_control') }}
            where source = 'dag_transform'
        )
    {% else %}
        1 = 1
    {% endif %}
{% endmacro %}
