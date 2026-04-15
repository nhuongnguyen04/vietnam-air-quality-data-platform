{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, district, month)',
    order_by='(province, month, assumeNotNull(district))',
    partition_by='toYYYYMM(month)'
) }}

with daily as (
    select 
        *,
        toStartOfMonth(date) as month
    from {{ ref('fct_air_quality_district_level_daily') }}
    {% if is_incremental() %}
    where month >= (select max(month) from {{ this }})
    {% endif %}
),

monthly as (
    select
        month,
        province,
        district,
        region_3,
        region_8,
        
        avg(avg_aqi_us) as avg_aqi_us,
        avg(avg_aqi_vn) as avg_aqi_vn,
        
        max(pm25_max) as pm25_max_ever,
        avg(pm25_avg) as pm25_avg,
        avg(pm10_avg) as pm10_avg,
        
        avg(co_avg)   as co_avg,
        avg(no2_avg)  as no2_avg,
        avg(so2_avg)  as so2_avg,
        avg(o3_avg)   as o3_avg,

        max(last_ingested_at) as last_ingested_at
    from daily
    group by
        month,
        province,
        district,
        region_3,
        region_8
)

select * from monthly
