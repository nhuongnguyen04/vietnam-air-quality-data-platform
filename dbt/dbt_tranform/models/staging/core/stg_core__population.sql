{{ config(
    materialized='view'
) }}

with ward_data as (
    select
        province,
        population,
        area_km2
    from {{ ref('stg_core__administrative_units') }}
),

province_agg as (
    select
        province as location_name,
        sum(population) as total_population,
        sum(area_km2) as total_area_km2,
        case 
            when sum(area_km2) > 0 then sum(population) / sum(area_km2)
            else 0
        end as density_per_km2
    from ward_data
    group by province
)

select
    location_name,
    null as original_location_name,
    total_population,
    total_area_km2 as area_km2,
    density_per_km2
from province_agg
