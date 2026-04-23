{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, ward_code, month)',
    order_by='(province, month, assumeNotNull(ward_code))',
    partition_by='toYYYYMM(month)'
) }}

with summary as (
    select * from {{ ref('fct_air_quality_summary_daily') }}
    {% if is_incremental() %}
    where date >= (select max(month) - interval 1 month from {{ this }})
    {% endif %}
),

pivoted as (
    select
        toStartOfMonth(date) as month,
        province,
        ward_code,
        any(region_3) as region_3,
        any(region_8) as region_8,
        source,
        
        -- Monthly averages
        avg(daily_avg_aqi_us) as monthly_avg_aqi_us,
        avg(daily_avg_aqi_vn) as monthly_avg_aqi_vn,
        
        -- Max/Min
        max(daily_max_aqi_us) as monthly_max_aqi_us,
        min(daily_avg_aqi_us) as monthly_min_aqi_us,
        
        -- Concentrations
        avg(pm25_daily_avg) as pm25_monthly_avg,
        avg(pm10_daily_avg) as pm10_monthly_avg,
        avg(co_daily_avg)   as co_monthly_avg,
        avg(no2_daily_avg)  as no2_monthly_avg,
        avg(so2_daily_avg)  as so2_monthly_avg,
        avg(o3_daily_avg)   as o3_monthly_avg,
        
        -- Dominant Pollutant (Most frequent in month)
        topK(1)(dominant_pollutant_vn)[1] as dominant_pollutant_vn
        
    from summary
    group by month, province, ward_code, source
)

select * from pivoted
