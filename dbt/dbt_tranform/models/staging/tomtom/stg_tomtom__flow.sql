{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(raw_loaded_at)',
    unique_key=['ward_code', 'timestamp_utc', 'parameter'],
    order_by='(ward_code, timestamp_utc, parameter)',
    partition_by='toYYYYMM(timestamp_utc)',
    query_settings={
        'max_threads': 1,
        'max_block_size': 4096,
        'max_bytes_before_external_sort': 67108864,
        'max_bytes_before_external_group_by': 67108864,
        'optimize_aggregation_in_order': 1
    }
) }}

WITH incremental_source AS (
    SELECT
        source,
        traffic_source,
        leftPad(toString(toInt64(toFloat64(assumeNotNull(ward_code)))), 5, '0') as ward_code,
        ward_name,
        province_name,
        latitude,
        longitude,
        toStartOfHour(timestamp_utc) as hourly_timestamp,
        current_travel_time,
        free_flow_travel_time,
        current_speed,
        free_flow_speed,
        confidence,
        ingest_time,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at
    FROM {{ source('tomtom', 'raw_tomtom_traffic') }}
    {{ staging_incremental_where('raw_sync_run_id', 'raw_loaded_at') }}
),

calculated_ratios AS (
    SELECT
        source,
        traffic_source,
        ward_code,
        ward_name,
        province_name,
        latitude,
        longitude,
        hourly_timestamp,
        current_travel_time,
        free_flow_travel_time,
        current_speed,
        free_flow_speed,
        confidence,
        ingest_time,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at,
        -- Formula: (current travel time - free flow travel time) / free flow travel time
        -- Fallback: (free flow speed / current speed) - 1
        CASE
            WHEN free_flow_travel_time > 0 AND current_travel_time > 0 
                THEN (CAST(current_travel_time, 'Float64') - CAST(free_flow_travel_time, 'Float64')) / CAST(free_flow_travel_time, 'Float64')
            WHEN free_flow_speed > 0 AND current_speed > 0
                THEN (CAST(free_flow_speed, 'Float64') / CAST(current_speed, 'Float64')) - 1
            ELSE 0
        END as raw_congestion_ratio
    FROM incremental_source
),

latest_per_hour AS (
    SELECT
        source,
        ward_code,
        ward_name,
        province_name,
        latitude,
        longitude,
        hourly_timestamp,
        traffic_source,
        confidence,
        ingest_time,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at,
        raw_congestion_ratio
    FROM calculated_ratios
    ORDER BY
        ward_code,
        hourly_timestamp,
        source,
        raw_loaded_at desc
    LIMIT 1 BY ward_code, hourly_timestamp, source
)

SELECT
    source,
    ward_code,
    ward_name,
    province_name,
    latitude,
    longitude,
    hourly_timestamp as timestamp_utc,
    clamp(raw_congestion_ratio, 0, 5) as value,
    'congestion_ratio' as parameter,
    traffic_source,
    confidence as quality_flag,
    ingest_time,
    raw_loaded_at,
    raw_sync_run_id,
    raw_sync_started_at,
    now() as dbt_updated_at
FROM latest_per_hour
