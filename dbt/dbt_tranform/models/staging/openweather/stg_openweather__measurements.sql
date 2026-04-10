{{ config(materialized='view') }}

with source as (
    select * from {{ source('openweather', 'raw_openweather_measurements') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by station_id, timestamp_utc, parameter
            order by ingest_time desc
        ) as rn
    from source
),

cleaned as (
    select
        'openweather' as source,
        city_name as district,
        city_name as province, -- OpenWeather city-centroid data, using city as both for now
        timestamp_utc,
        parameter,
        value,
        aqi_reported,
        'valid' as quality_flag,
        ingest_time
    from deduplicated
    where rn = 1
)

select * from cleaned
