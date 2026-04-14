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
        -- Using seed-based mapping for consistent, accented names
        coalesce(pn.target_name, u.province) as province,
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

with_regions as (
    select
        *,
        {{ get_vietnam_region_3('province') }} as region_3,
        {{ get_vietnam_region_8('province') }} as region_8
    from normalized
)

select * from with_regions
