{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, date)',
    order_by='(province, date)',
    partition_by='toYYYYMM(date)'
) }}

with ward_daily as (
    select * from {{ ref('fct_air_quality_ward_level_daily') }}
    {% if is_incremental() %}
    where date >= (select max(date) - interval 2 day from {{ this }})
    {% endif %}
),

daily as (
    select
        date,
        province,
        region_3,
        region_8,
        
        avg(daily_avg_aqi_us) as prov_avg_aqi_us,
        avg(daily_avg_aqi_vn) as prov_avg_aqi_vn,
        
        -- Daily metrics for the province
        max(pm25_daily_avg) as pm25_prov_max,
        avg(pm25_daily_avg) as pm25_prov_avg,
        avg(pm10_daily_avg) as pm10_prov_avg,
        
        avg(co_daily_avg)   as co_prov_avg,
        avg(no2_daily_avg)  as no2_prov_avg,
        avg(so2_daily_avg)  as so2_prov_avg,
        avg(o3_daily_avg)   as o3_prov_avg,

        max(last_ingested_at) as last_ingested_at
        
    from ward_daily
    group by 1, 2, 3, 4
)

select * from daily
