{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, ward_code, month)',
    order_by='(province, month, assumeNotNull(ward_code))',
    partition_by='toYYYYMM(month)'
) }}

with ward_monthly as (
    select * from {{ ref('fct_air_quality_ward_level_monthly') }}
    {% if is_incremental() %}
    where month >= (select max(month) - interval 1 month from {{ this }})
    {% endif %}
),

admin_units as (
    select 
        ward_code,
        ward_name,
        province,
        region_3,
        region_8,
        latitude,
        longitude,
        population
    from {{ ref('dim_administrative_units') }}
),

final as (
    select
        w.month as month,
        a.province as province,
        a.ward_code as ward_code,
        a.ward_name as ward_name,
        a.region_3 as region_3,
        a.region_8 as region_8,
        a.latitude as latitude,
        a.longitude as longitude,
        a.population as population,
        
        -- Standardized columns for dashboard
        w.avg_aqi_us as avg_aqi_us,
        w.max_aqi_us as max_aqi_us,
        w.avg_aqi_vn as avg_aqi_vn,
        w.max_aqi_vn as max_aqi_vn,
        w.main_pollutant as main_pollutant,
        
        -- Standardized concentrations
        w.pm25_avg as pm25_avg,
        w.pm10_avg as pm10_avg,
        w.co_avg as co_avg,
        w.no2_avg as no2_avg,
        w.so2_avg as so2_avg,
        w.o3_avg as o3_avg,
        
        w.samples_count as samples_count,
        w.last_ingested_at as last_ingested_at
    from ward_monthly w
    left join admin_units a on w.ward_code = a.ward_code
)

select * from final
