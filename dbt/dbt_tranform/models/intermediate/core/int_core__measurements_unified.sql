with aqiin as (
    select
        source,
        station_name,
        district,
        province,
        timestamp_utc,
        parameter,
        value,
        aqi_reported,
        quality_flag,
        ingest_time
    from {{ ref('stg_aqiin__measurements') }}
),

openweather as (
    select
        source,
        station_name,
        district,
        province,
        timestamp_utc,
        parameter,
        value,
        aqi_reported,
        quality_flag,
        ingest_time
    from {{ ref('stg_openweather__measurements') }}
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
