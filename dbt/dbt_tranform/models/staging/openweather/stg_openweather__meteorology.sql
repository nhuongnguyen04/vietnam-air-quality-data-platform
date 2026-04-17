{{ config(
    engine='ReplacingMergeTree',
    unique_key='(ward_code, timestamp_utc)',
    order_by='(province, timestamp_utc, ward_code)',
    partition_by='toYYYYMM(timestamp_utc)'
) }}

with raw_data as (
    select
        source,
        cluster_lat,
        cluster_lon,
        province_name,
        toStartOfHour(timestamp_utc) as hourly_timestamp,
        temp,
        temp_min,
        temp_max,
        feels_like,
        humidity,
        pressure,
        visibility,
        wind_speed,
        wind_deg,
        clouds_all,
        ingest_time
    from {{ source('openweather', 'raw_openweather_meteorology') }}
),

-- Step 1: For each (province, cluster) find the nearest ward.
-- Computing greatCircleDistance inline inside argMin keeps it out of GROUP BY state.
-- Province-filtered subquery replaces cross join to satisfy ClickHouse syntax.
nearest_ward as (
    select
        r.province_name,
        r.cluster_lat,
        r.cluster_lon,
        r.hourly_timestamp,
        r.ingest_time,
        argMin(ward_code, greatCircleDistance(r.cluster_lon, r.cluster_lat, admin.ward_lon, admin.ward_lat)) as nearest_ward_code,
        argMin(province,     greatCircleDistance(r.cluster_lon, r.cluster_lat, admin.ward_lon, admin.ward_lat)) as province
    from raw_data r
    inner join (
        select ward_code, province, lat as ward_lat, lon as ward_lon
        from {{ ref('stg_core__administrative_units') }}
    ) admin on r.province_name = admin.province
    group by r.province_name, r.cluster_lat, r.cluster_lon, r.hourly_timestamp, r.ingest_time
),

-- Step 2: Attach meteorological columns to nearest ward (simple left join, no heavy functions)
mapped_data as (
    select
        r.source,
        nw.nearest_ward_code as ward_code,
        nw.province,
        r.cluster_lat as latitude,
        r.cluster_lon as longitude,
        r.hourly_timestamp as timestamp_utc,
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
        r.ingest_time
    from raw_data r
    inner join nearest_ward nw
        on  r.province_name = nw.province_name
        and r.cluster_lat = nw.cluster_lat
        and r.cluster_lon = nw.cluster_lon
        and r.hourly_timestamp = nw.hourly_timestamp
        and r.ingest_time = nw.ingest_time
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by ward_code, timestamp_utc
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
    timestamp_utc,
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
