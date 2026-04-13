{{ config(
    materialized='view'
) }}

WITH raw_data AS (
    SELECT * FROM {{ source('openweather', 'raw_openweather_meteorology') }}
),

deduplicated AS (
    SELECT
        source,
        province,
        latitude,
        longitude,
        toStartOfHour(timestamp_utc) as timestamp_utc,
        temp,
        feels_like,
        humidity,
        pressure,
        wind_speed,
        wind_deg,
        clouds_all,
        ingest_time,
        -- Keep most recent ingest for each province and hour
        ROW_NUMBER() OVER (PARTITION BY province, toStartOfHour(timestamp_utc) ORDER BY ingest_time DESC) as rn
    FROM raw_data
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
WHERE rn = 1
