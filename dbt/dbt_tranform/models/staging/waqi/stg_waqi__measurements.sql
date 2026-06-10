{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree(raw_loaded_at)',
    incremental_strategy='append',
    unique_key='(station_name, timestamp_utc, parameter)',
    order_by='(timestamp_utc, station_name, parameter)',
    partition_by='toYYYYMM(timestamp_utc)',
    query_settings={
        'max_threads': 1,
        'max_block_size': 4096,
        'max_bytes_before_external_sort': 1073741824,
        'max_bytes_before_external_group_by': 1073741824,
        'max_memory_usage': 6442450944
    }
) }}

with incremental_source as (
    select
        station_name,
        timestamp_utc,
        CAST(parameter AS String) as parameter,
        value,
        CAST(unit AS String) as unit,
        aqi_reported,
        CAST(quality_flag AS String) as quality_flag,
        ingest_time,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at
    from {{ source('waqi', 'raw_waqi_measurements') }}
    {{ staging_incremental_where('raw_sync_run_id', 'raw_loaded_at') }}
),

latest_rows as (
    select
        station_name,
        timestamp_utc,
        parameter,
        argMax(value, raw_loaded_at) as raw_value,
        argMax(unit, raw_loaded_at) as raw_unit,
        argMax(aqi_reported, raw_loaded_at) as aqi_reported,
        argMax(quality_flag, raw_loaded_at) as quality_flag,
        argMax(ingest_time, raw_loaded_at) as latest_ingest_time,
        max(raw_loaded_at) as latest_raw_loaded_at,
        argMax(raw_sync_run_id, raw_loaded_at) as latest_raw_sync_run_id,
        argMax(raw_sync_started_at, raw_loaded_at) as latest_raw_sync_started_at
    from incremental_source
    group by
        station_name,
        timestamp_utc,
        parameter
),

normalized as (
    select
        'waqi' as source,
        station_name,
        timestamp_utc,
        parameter,
        case
            when raw_unit = 'ppb' then
                case
                    when parameter = 'o3' then raw_value * 1.96
                    when parameter = 'no2' then raw_value * 1.88
                    when parameter = 'so2' then raw_value * 2.62
                    when parameter = 'co' then raw_value * 1.145
                    else raw_value
                end
            when raw_unit = 'ppm' and parameter = 'co' then
                case
                    when raw_value >= 10 then raw_value * 1.145
                    else raw_value * 1145
                end
            else raw_value
        end as value,
        aqi_reported,
        'µg/m³' as unit,
        quality_flag,
        latest_ingest_time as ingest_time,
        latest_raw_loaded_at as raw_loaded_at,
        latest_raw_sync_run_id as raw_sync_run_id,
        latest_raw_sync_started_at as raw_sync_started_at
    from latest_rows
)

select * from normalized
