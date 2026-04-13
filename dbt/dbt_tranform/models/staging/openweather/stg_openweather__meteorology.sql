{{ config(
    materialized='view'
) }}

WITH raw_data AS (
    SELECT * FROM {{ source('openweather', 'raw_openweather_meteorology') }}
),

deduplicated AS (
    SELECT
        province,
        toStartOfHour(timestamp_utc) as timestamp_utc,
        argMax(source, ingest_time) as source,
        argMax(latitude, ingest_time) as latitude,
        argMax(longitude, ingest_time) as longitude,
        argMax(temp, ingest_time) as temp,
        argMax(feels_like, ingest_time) as feels_like,
        argMax(humidity, ingest_time) as humidity,
        argMax(pressure, ingest_time) as pressure,
        argMax(wind_speed, ingest_time) as wind_speed,
        argMax(wind_deg, ingest_time) as wind_deg,
        argMax(clouds_all, ingest_time) as clouds_all,
        max(ingest_time) as ingest_time
    FROM raw_data
    GROUP BY province, timestamp_utc
)

SELECT
    source,
    province,
    latitude,
    longitude,
    timestamp_utc,
    temp,
    feels_like,
    humidity,
    pressure,
    wind_speed,
    wind_deg,
    clouds_all,
    now() as dbt_updated_at
FROM deduplicated
