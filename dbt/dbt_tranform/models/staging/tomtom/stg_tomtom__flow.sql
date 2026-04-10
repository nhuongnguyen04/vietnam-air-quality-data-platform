{{ config(
    materialized='view'
) }}

WITH raw_data AS (
    -- Use the Python-calculated hourly table as primary source
    SELECT * FROM {{ source('tomtom', 'raw_tomtom_traffic_hourly') }}
),

deduplicated AS (
    SELECT
        source,
        station_name,
        latitude,
        longitude,
        hour_utc,
        congestion_ratio,
        data_quality_flag,
        updated_at,
        ROW_NUMBER() OVER (PARTITION BY station_name, hour_utc ORDER BY updated_at DESC) as rn
    FROM raw_data
)

SELECT
    source,
    station_name,
    latitude,
    longitude,
    hour_utc as timestamp_utc,
    congestion_ratio as value,
    'congestion_ratio' as parameter,
    data_quality_flag as quality_flag,
    updated_at as ingest_time,
    now() as dbt_updated_at
FROM deduplicated
WHERE rn = 1
