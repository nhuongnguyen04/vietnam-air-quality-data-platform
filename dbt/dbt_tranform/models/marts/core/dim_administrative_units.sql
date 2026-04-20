{{ config(
    materialized='table',
    engine='MergeTree',
    order_by='ward_code'
) }}

with admin_source as (
    select
        ward_code,
        ward_name,
        province,
        lat as latitude,
        lon as longitude,
        population,
        area_km2
    from {{ ref('stg_core__administrative_units') }}
),

final as (
    select
        ward_code,
        ward_name,
        province,
        {{ get_vietnam_region_3('province') }} as region_3,
        {{ get_vietnam_region_8('province') }} as region_8,
        latitude,
        longitude,
        population,
        area_km2
    from admin_source
)

select * from final
