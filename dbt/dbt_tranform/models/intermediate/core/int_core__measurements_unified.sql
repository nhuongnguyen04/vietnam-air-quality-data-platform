{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    incremental_strategy='append',
    unique_key='(source, ward_code, timestamp_utc, parameter)',
    order_by='(province, timestamp_utc, ward_code, source, parameter)',
    partition_by='toYYYYMM(timestamp_utc)',
    query_settings={
        'max_threads': 2
    }
) }}

with aqiin as (
    select
        m.source,
        s.ward_code,
        s.province,
        s.latitude,
        s.longitude,
        m.timestamp_utc,
        m.parameter,
        m.value,
        m.aqi_reported,
        m.quality_flag,
        m.ingest_time
    from {{ ref('stg_aqiin__measurements') }} m
    join {{ ref('stg_core__stations') }} s on m.station_name = s.station_name
    {% if is_incremental() %}
    where m.ingest_time > (select max(ingest_time) from {{ this }})
    {% endif %}
),

openweather as (
    select
        source,
        ward_code,
        province,
        latitude,
        longitude,
        timestamp_utc,
        parameter,
        value,
        aqi_reported,
        quality_flag,
        ingest_time
    from {{ ref('stg_openweather__measurements') }}
    {% if is_incremental() %}
    where ingest_time > (select max(ingest_time) from {{ this }})
    {% endif %}
),

unified as (
    select * from aqiin
    union all
    select * from openweather
),

admin_units as (
    select
        ward_code,
        province,
        lat as ward_lat,
        lon as ward_lon
    from {{ ref('stg_core__administrative_units') }}
),

-- Identify physical stations and their distance to ward centroids
ward_station_flags as (
    select
        s.ward_code,
        min(greatCircleDistance(s.longitude, s.latitude, a.ward_lon, a.ward_lat)) as nearest_station_distance_m,
        min(greatCircleDistance(s.longitude, s.latitude, a.ward_lon, a.ward_lat)) <= 2000 as has_nearby_aqiin_station
    from {{ ref('stg_core__stations') }} s
    join admin_units a on s.ward_code = a.ward_code
    where s.station_source = 'aqiin'
    group by s.ward_code
),

-- Rule: In the same ward, at the same time, if there is a physical station (Aqiin)
-- that is close (<= 2000m) to the ward centroid, we drop the OpenWeather (satellite) data.
-- If the physical station is far (> 2000m), we keep both.
filtered as (
    select
        u.source,
        u.ward_code,
        u.province,
        u.latitude,
        u.longitude,
        u.timestamp_utc,
        u.parameter,
        u.value,
        u.aqi_reported,
        u.quality_flag,
        u.ingest_time
    from unified u
    left join ward_station_flags w on u.ward_code = w.ward_code
    -- We filter OUT OpenWeather records ONLY IF there is a nearby AQI.in station
    -- for the same ward centroid.
    where not (
        u.source = 'openweather'
        and coalesce(w.has_nearby_aqiin_station, false)
    )
),

with_regions as (
    select
        f.source,
        f.ward_code,
        f.province,
        f.latitude,
        f.longitude,
        f.timestamp_utc,
        f.parameter,
        f.value,
        f.aqi_reported,
        f.quality_flag,
        f.ingest_time,
        {{ get_vietnam_region_3('f.province') }} as region_3,
        {{ get_vietnam_region_8('f.province') }} as region_8
    from filtered f
),

calibrated as (
    select
        *,
        case 
            when source = 'aqiin' then 5 
            else 1 
        end as source_weight,
        -- Apply calibration factors for OpenWeather to align with ground truth
        case
            when source = 'openweather' then
                case 
                    when parameter = 'o3' then value * 0.20
                    when parameter = 'no2' then value * 2.00
                    when parameter = 'pm25' then value * 0.80
                    when parameter = 'co' then value * 0.80
                    else value
                end
            else value
        end as calibrated_value
    from with_regions
)

select 
    source,
    assumeNotNull(ward_code) as ward_code,
    assumeNotNull(province) as province,
    latitude,
    longitude,
    timestamp_utc,
    parameter,
    value as raw_value,
    calibrated_value as value,
    source_weight,
    aqi_reported,
    quality_flag,
    ingest_time,
    region_3,
    region_8
from calibrated
