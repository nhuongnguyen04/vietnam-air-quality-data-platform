{{ config(materialized='view') }}

with source as (
    select
        province,
        ward,
        code as ward_code,
        lat,
        lon,
        population,
        area_km2
    from {{ ref('vietnam_wards_with_osm') }}
),

renamed as (
    select
        leftPad(toString(toInt64(toFloat64(assumeNotNull(ward_code)))), 5, '0') as ward_code,
        ward as ward_name,
        province,
        lat,
        lon,
        population,
        area_km2
    from source
)

select * from renamed
