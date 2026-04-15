{{ config(
    materialized='view'
) }}

WITH raw_data AS (
    SELECT * FROM {{ source('openweather', 'raw_openweather_meteorology') }}
),

ingestion_points as (
    SELECT * FROM {{ ref('openweather_ingestion_points') }}
),

province_norm as (
    SELECT * FROM {{ ref('province_normalization') }}
),

deduplicated AS (
    SELECT
        r.source,
        COALESCE(p.province, r.province) as raw_province,
        COALESCE(p.district, 'Unknown') as district,
        r.latitude,

        r.longitude,
        toStartOfHour(r.timestamp_utc) as hourly_timestamp,
        r.temp,
        r.feels_like,
        r.humidity,
        r.pressure,
        r.wind_speed,
        r.wind_deg,
        r.clouds_all,
        r.ingest_time,
        -- Window function to keep latest record
        row_number() over (
            partition by r.latitude, r.longitude, toStartOfHour(r.timestamp_utc)
            order by r.ingest_time desc
        ) as rn
    FROM raw_data r
    LEFT JOIN ingestion_points p ON 
        toDecimal32(r.latitude, 4) = toDecimal32(p.latitude, 4) and 
        toDecimal32(r.longitude, 4) = toDecimal32(p.longitude, 4)
)

SELECT
    d.source,
    COALESCE(pn.target_name, d.raw_province) as province,
    d.district,
    d.latitude,
    d.longitude,
    d.hourly_timestamp as timestamp_utc,
    d.temp,
    d.feels_like,
    d.humidity,
    d.pressure,
    d.wind_speed,
    d.wind_deg,
    d.clouds_all,
    now() as dbt_updated_at
FROM deduplicated d
LEFT JOIN province_norm pn ON d.raw_province = pn.raw_name
WHERE d.rn = 1
