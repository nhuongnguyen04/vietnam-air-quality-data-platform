{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree(ingest_time)',
    incremental_strategy='delete_insert',
    unique_key=['ward_code', 'timestamp_utc', 'parameter'],
    order_by='(ward_code, timestamp_utc, parameter)',
    partition_by='toYYYYMM(timestamp_utc)',
    query_settings={
        'max_threads': 1,
        'max_bytes_before_external_sort': 67108864,
        'max_bytes_before_external_group_by': 67108864,
        'optimize_aggregation_in_order': 1
    }
) }}

with incremental_source as (
    select
        ward_code,
        ward_name,
        province_name,
        latitude,
        longitude,
        timestamp_utc,
        CAST(parameter AS String) as parameter,
        value,
        aqi_reported,
        ingest_time
    from {{ source('openweather', 'raw_openweather_measurements') }}
    {% if is_incremental() %}
    where ingest_time >= (
        select max(ingest_time) - interval 24 hour
        from {{ this }}
    )
    {% endif %}
),

deduplicated as (
    select
        ward_code,
        timestamp_utc,
        parameter,
        argMax(ward_name, ingest_time) as ward_name,
        argMax(province_name, ingest_time) as province_name,
        argMax(latitude, ingest_time) as latitude,
        argMax(longitude, ingest_time) as longitude,
        argMax(value, ingest_time) as value,
        argMax(aqi_reported, ingest_time) as aqi_reported,
        max(ingest_time) as latest_ingest_time
    from incremental_source
    group by
        ward_code,
        timestamp_utc,
        parameter
),

cleaned as (
    select
        'openweather' as source,
        ward_code,
        ward_name,
        province_name as province,
        latitude,
        longitude,
        timestamp_utc,
        parameter,
        value,
        aqi_reported,
        'valid' as quality_flag,
        latest_ingest_time as ingest_time
    from deduplicated
)

select * from cleaned
