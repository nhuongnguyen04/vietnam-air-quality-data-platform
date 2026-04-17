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
        m.ingest_time,
        s.station_name -- Keep for internal prioritization logic
    from {{ ref('stg_aqiin__measurements') }} m
    join {{ ref('stg_core__stations') }} s on m.station_name = s.station_name
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
        ingest_time,
        '' as station_name
    from {{ ref('stg_openweather__measurements') }}
),

unified as (
    select * from aqiin
    union all
    select * from openweather
),

admin_units as (
    select
        ward_code,
        ward_name,
        province,
        lat as ward_lat,
        lon as ward_lon
    from {{ ref('stg_core__administrative_units') }}
),

-- Identify physical stations and their distance to ward centroids
physical_station_info as (
    select distinct
        s.ward_code,
        s.station_name,
        greatCircleDistance(s.longitude, s.latitude, a.ward_lon, a.ward_lat) as dist_to_centroid
    from {{ ref('stg_core__stations') }} s
    join admin_units a on s.ward_code = a.ward_code
    where s.station_source = 'aqiin'
),

-- Rule: In the same ward, at the same time, if there is a physical station (Aqiin) 
-- that is close (<= 2000m) to the ward centroid, we drop the OpenWeather (satellite) data.
-- If the physical station is far (> 2000m), we keep both.
filtered as (
    select 
        u.*
    from unified u
    left join physical_station_info p on u.ward_code = p.ward_code
    -- We filter OUT OpenWeather records if:
    -- There is an Aqiin station for that ward AND it's within 2km of centroid
    where not (
        u.source = 'openweather' 
        and p.station_name is not null 
        and p.dist_to_centroid <= 2000
    )
),

with_regions as (
    select
        f.*,
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
