{{ config(materialized='view') }}

with admin_units as (
    select
        ward_code,
        ward_name,
        province,
        lat as ward_lat,
        lon as ward_lon
    from {{ ref('stg_core__administrative_units') }}
),

aqicn_stations as (
    select 
        station_name,
        latitude,
        longitude,
        -- Normalize province names for joining
        case 
            when province = 'TP.Hà Nội' then 'Hà Nội'
            when province = 'TP.Hồ Chí Minh' then 'Thành phố Hồ Chí Minh'
            when province = 'TP.Đà Nẵng' then 'Đà Nẵng'
            when province = 'TP.Hải Phòng' then 'Hải Phòng'
            when province = 'TP.Cần Thơ' then 'Cần Thơ'
            else province
        end as normalized_province,
        ward as source_ward_name
    from {{ ref('unified_stations_metadata') }}
),

-- 1. Try to map by name
mapped_by_name as (
    select
        s.station_name,
        s.latitude,
        s.longitude,
        'aqiin' as station_source,
        a.ward_code,
        a.province
    from aqicn_stations s
    left join admin_units a 
        on s.normalized_province = a.province 
        and s.source_ward_name = a.ward_name
),

-- 2. Fallback for those that didn't match by name
fallback_needed as (
    select * from mapped_by_name where ward_code is null
),

mapped_by_distance as (
    select
        f.station_name,
        f.latitude,
        f.longitude,
        f.station_source,
        argMin(admin.ward_code, greatCircleDistance(f.longitude, f.latitude, admin.ward_lon, admin.ward_lat)) as ward_code,
        argMin(admin.province, greatCircleDistance(f.longitude, f.latitude, admin.ward_lon, admin.ward_lat)) as province
    from fallback_needed f
    cross join admin_units admin
    group by 1, 2, 3, 4
),

-- Unify Aqiin stations
unified_aqiin as (
    select * from mapped_by_name where ward_code is not null
    union all
    select * from mapped_by_distance
)

select
    assumeNotNull(station_name) as station_name,
    latitude,
    longitude,
    station_source,
    assumeNotNull(ward_code) as ward_code,
    assumeNotNull(province) as province
from unified_aqiin
