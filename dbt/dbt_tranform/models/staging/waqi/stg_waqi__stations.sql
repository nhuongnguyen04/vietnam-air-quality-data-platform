{{ config(materialized='view') }}

with admin_units as (
    select
        ward_code,
        province,
        lat as ward_lat,
        lon as ward_lon
    from {{ ref('stg_core__administrative_units') }}
),

waqi_raw_stations as (
    select
        station_name,
        latitude,
        longitude
    from {{ source('waqi', 'dim_waqi_stations') }}
    group by station_name, latitude, longitude
),

mapped_by_distance as (
    select
        w.station_name,
        w.latitude,
        w.longitude,
        'waqi' as station_source,
        argMin(admin.ward_code, greatCircleDistance(w.longitude, w.latitude, admin.ward_lon, admin.ward_lat)) as ward_code,
        argMin(admin.province, greatCircleDistance(w.longitude, w.latitude, admin.ward_lon, admin.ward_lat)) as province
    from waqi_raw_stations w
    cross join admin_units admin
    group by 1, 2, 3, 4
)

select
    assumeNotNull(station_name) as station_name,
    latitude,
    longitude,
    station_source,
    assumeNotNull(ward_code) as ward_code,
    assumeNotNull(province) as province
from mapped_by_distance
