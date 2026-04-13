{{ config(
    materialized='view'
) }}

WITH raw_data AS (
    -- Use the Python-calculated hourly table as primary source
    SELECT * FROM {{ source('tomtom', 'raw_tomtom_traffic_hourly') }}
),

deduplicated AS (
    SELECT
        station_name,
        hour_utc,
        argMax(source, updated_at) as source,
        argMax(latitude, updated_at) as latitude,
        argMax(longitude, updated_at) as longitude,
        argMax(congestion_ratio, updated_at) as congestion_ratio,
        argMax(data_quality_flag, updated_at) as data_quality_flag,
        max(updated_at) as updated_at
    FROM raw_data
    GROUP BY station_name, hour_utc
)

SELECT
    source,
    station_name,
    latitude,
    longitude,
    hour_utc as timestamp_utc,
    clamp(congestion_ratio, 0, 1) as value,
    'congestion_ratio' as parameter,
    data_quality_flag as quality_flag,
    updated_at as ingest_time,
    now() as dbt_updated_at
FROM deduplicated
