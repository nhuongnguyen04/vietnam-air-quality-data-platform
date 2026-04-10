{{ config(
    materialized='incremental',
    unique_key='(province, district, month)',
    order_by='(province, month, assumeNotNull(district))',
    partition_by='toYYYYMM(month)'
) }}

with daily_summary as (
    select * from {{ ref('fct_air_quality_summary_daily') }}
    {% if is_incremental() %}
    where date >= toStartOfMonth(today()) - interval 1 month
    {% endif %}
)

select
    toDate(toStartOfMonth(date)) as month,
    province,
    district,
    region_3,
    region_8,
    source,
    
    -- Overall AQIs
    round(avg(avg_aqi_us), 2) as avg_aqi_us,
    round(avg(avg_aqi_vn), 2) as avg_aqi_vn,
    max(max_aqi_us) as max_aqi_us,
    max(max_aqi_vn) as max_aqi_vn,
    
    -- Concentrations (Monthly Averages)
    round(avg(pm25_avg), 2) as pm25_avg,
    round(avg(pm10_avg), 2) as pm10_avg,
    round(avg(co_avg), 2)   as co_avg,
    round(avg(no2_avg), 2)  as no2_avg,
    round(avg(so2_avg), 2)  as so2_avg,
    round(avg(o3_avg), 2)   as o3_avg,
    
    count(*) as day_count
    
from daily_summary
group by 1, 2, 3, 4, 5, 6
