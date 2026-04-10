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
        s.province,
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

with_regions as (
    select
        *,
        {{ get_vietnam_region_3('province') }} as region_3,
        {{ get_vietnam_region_8('province') }} as region_8
    from unified
)

select * from with_regions
