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
        ingest_time,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at
    from {{ source('openweather', 'raw_openweather_measurements') }}
    {{ staging_incremental_where('raw_sync_run_id', 'raw_loaded_at') }}
),

deduplicated as (
    select
        ward_code,
        timestamp_utc,
        parameter,
        argMax(ward_name, raw_loaded_at) as ward_name,
        argMax(province_name, raw_loaded_at) as province_name,
        argMax(latitude, raw_loaded_at) as latitude,
        argMax(longitude, raw_loaded_at) as longitude,
        argMax(value, raw_loaded_at) as value,
        argMax(aqi_reported, raw_loaded_at) as aqi_reported,
        argMax(ingest_time, raw_loaded_at) as latest_ingest_time,
        max(raw_loaded_at) as latest_raw_loaded_at,
        argMax(raw_sync_run_id, raw_loaded_at) as latest_raw_sync_run_id,
        argMax(raw_sync_started_at, raw_loaded_at) as latest_raw_sync_started_at
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
        latest_ingest_time as ingest_time,
        latest_raw_loaded_at as raw_loaded_at,
        latest_raw_sync_run_id as raw_sync_run_id,
        latest_raw_sync_started_at as raw_sync_started_at
    from deduplicated
)

select * from cleaned
