with aqiin as (
    select
        m.source,
        m.station_name,
        s.district,
        s.province,
        m.timestamp_utc,
        m.parameter,
        m.value,
        m.aqi_reported,
        m.quality_flag,
        m.ingest_time
    from {{ ref('stg_aqiin__measurements') }} m
    left join {{ ref('stg_core__stations') }} s on m.station_name = s.station_name
),

openweather as (
    select
        m.source,
        m.station_name,
        s.district,
        -- Fallback to extracting from station_name: 'openweather:Province:Lat:Lon'
        -- Note: s.province is String (not Nullable), so LEFT JOIN results in '' if no match
        if(s.province != '', s.province, splitByChar(':', m.station_name)[2]) as province,
        m.timestamp_utc,
        m.parameter,
        m.value,
        m.aqi_reported,
        m.quality_flag,
        m.ingest_time
    from {{ ref('stg_openweather__measurements') }} m
    left join {{ ref('stg_core__stations') }} s on m.station_name = s.station_name
),

unified as (
    select * from aqiin
    union all
    select * from openweather
),

normalized as (
    select
        u.source,
        u.station_name,
        u.district,
        -- Apply normalization first
        coalesce(pn.target_name, u.province) as normalized_province,
        u.timestamp_utc,
        u.parameter,
        u.value,
        u.aqi_reported,
        u.quality_flag,
        u.ingest_time
    from unified u
    left join {{ ref('province_normalization') }} pn on u.province = pn.raw_name
    where u.province is not null and u.province != ''
),

mapped as (
    select
        n.*,
        -- Then map legacy provinces to the 34 target units
        coalesce(pm.target_unit_34, n.normalized_province) as province
    from normalized n
    left join {{ ref('province_to_unit_34') }} pm on n.normalized_province = pm.legacy_province
),

filtered as (
    -- Strict alignment with the 34 target provinces of the 2026 scope
    -- All legacy data has been mapped to these 34 parents
    select * from mapped
    where province in (
        select distinct province from {{ ref('openweather_ingestion_points') }}
    )
),

with_weights as (
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
        end as calibrated_value,
        {{ get_vietnam_region_3('province') }} as region_3,
        {{ get_vietnam_region_8('province') }} as region_8
    from filtered
)

select 
    source,
    station_name,
    district,
    province,
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
from with_weights
