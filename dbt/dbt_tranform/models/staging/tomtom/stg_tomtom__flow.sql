{{ config(
    engine='ReplacingMergeTree',
    unique_key='(ward_code, timestamp_utc, parameter)',
    order_by='(province_name, timestamp_utc, ward_code)',
    partition_by='toYYYYMM(timestamp_utc)'
) }}

WITH raw_data AS (
    SELECT
        source,
        traffic_source,
        ward_code,
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
        ingest_time
    FROM {{ source('tomtom', 'raw_tomtom_traffic') }}
),

calculated_ratios AS (
    SELECT
        *,
        -- Formula: (current travel time - free flow travel time) / free flow travel time
        -- Fallback: (free flow speed / current speed) - 1
        CASE
            WHEN free_flow_travel_time > 0 AND current_travel_time > 0 
                THEN (CAST(current_travel_time, 'Float64') - CAST(free_flow_travel_time, 'Float64')) / CAST(free_flow_travel_time, 'Float64')
            WHEN free_flow_speed > 0 AND current_speed > 0
                THEN (CAST(free_flow_speed, 'Float64') / CAST(current_speed, 'Float64')) - 1
            ELSE 0
        END as raw_congestion_ratio
    FROM raw_data
),

hourly_aggregated AS (
    SELECT
        source,
        ward_code,
        hourly_timestamp,
        -- Use argMax to get the coordinates associated with the most recent sample
        argMax(latitude, ingest_time) as latitude,
        argMax(longitude, ingest_time) as longitude,
        avg(raw_congestion_ratio) as avg_congestion_ratio,
        avg(confidence) as avg_confidence,
        argMax(traffic_source, ingest_time) as primary_traffic_source,
        argMax(ward_name, ingest_time) as ward_name,
        argMax(province_name, ingest_time) as province_name,
        max(ingest_time) as max_ingest_time
    FROM calculated_ratios
    GROUP BY
        source,
        ward_code,
        hourly_timestamp
)

SELECT
    source,
    ward_code,
    ward_name,
    province_name,
    latitude,
    longitude,
    hourly_timestamp as timestamp_utc,
    clamp(avg_congestion_ratio, 0, 5) as value,
    'congestion_ratio' as parameter,
    primary_traffic_source as traffic_source,
    avg_confidence as quality_flag,
    max_ingest_time as ingest_time,
    now() as dbt_updated_at
FROM hourly_aggregated
