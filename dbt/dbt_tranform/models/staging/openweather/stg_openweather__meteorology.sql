{{ config(
    materialized='view'
) }}

with raw_data as (
    select * from {{ source('openweather', 'raw_openweather_meteorology') }}
),

admin_units as (
    select
        ward_code,
        province,
        lat as ward_lat,
        lon as ward_lon
    from {{ ref('stg_core__administrative_units') }}
),

-- Map meteorological data to the nearest ward using cluster centroids
-- Optimized: Join on province name first to reduce spatial search space significantly
mapped_data as (
    select
        r.source,
        r.cluster_lat as latitude,
        r.cluster_lon as longitude,
        toStartOfHour(r.timestamp_utc) as hourly_timestamp,
        r.temp,
        r.temp_min,
        r.temp_max,
        r.feels_like,
        r.humidity,
        r.pressure,
        r.visibility as visibility_meters,
        r.wind_speed,
        r.wind_deg,
        r.clouds_all,
        r.ingest_time,
        -- Find the nearest ward code WITHIN the same province
        argMin(admin.ward_code, greatCircleDistance(r.cluster_lon, r.cluster_lat, admin.ward_lon, admin.ward_lat)) as ward_code,
        admin.province
    from raw_data r
    inner join admin_units admin on r.province_name = admin.province
    group by 
        r.source,
        latitude,
        longitude,
        hourly_timestamp,
        r.temp,
        r.temp_min,
        r.temp_max,
        r.feels_like,
        r.humidity,
        r.pressure,
        visibility_meters,
        r.wind_speed,
        r.wind_deg,
        r.clouds_all,
        r.ingest_time,
        admin.province
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by ward_code, hourly_timestamp
            order by ingest_time desc
        ) as rn
    from mapped_data
)

select
    source,
    ward_code,
    province,
    latitude,
    longitude,
    hourly_timestamp as timestamp_utc,
    temp,
    temp_min,
    temp_max,
    feels_like,
    humidity,
    pressure,
    visibility_meters,
    wind_speed,
    wind_deg,
    clouds_all,
    now() as dbt_updated_at
from deduplicated
where rn = 1
