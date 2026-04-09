{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, district, date)',
    order_by='(province, date, assumeNotNull(district))',
    partition_by='toYYYYMM(date)'
) }}

with hourly_summary as (
    select * from {{ ref('fct_air_quality_summary_hourly') }}
    {% if is_incremental() %}
    where date >= (select max(date) - interval 2 day from {{ this }})
    {% endif %}
)

select
    date,
    province,
    district,
    region_3,
    region_8,
    source,
    
    -- Overall AQIs (Avg and Max of the day)
    round(avg(final_aqi_us), 2) as avg_aqi_us,
    round(avg(final_aqi_vn), 2) as avg_aqi_vn,
    max(final_aqi_us) as max_aqi_us,
    max(final_aqi_vn) as max_aqi_vn,
    
    -- Concentrations (Daily Averages)
    round(avg(pm25_value), 2) as pm25_avg,
    round(avg(pm10_value), 2) as pm10_avg,
    round(avg(co_value), 2)   as co_avg,
    round(avg(no2_value), 2)  as no2_avg,
    round(avg(so2_value), 2)  as so2_avg,
    round(avg(o3_value), 2)   as o3_avg,
    
    -- Max Concentrations
    max(pm25_value) as pm25_max,
    max(pm10_value) as pm10_max,
    
    -- Dominant Pollutant for the day (based on highest hourly AQI)
    argMax(dominant_pollutant_us, final_aqi_us) as dominant_pollutant_us,
    argMax(dominant_pollutant_vn, final_aqi_vn) as dominant_pollutant_vn,
    
    count(*) as hourly_count
    
from hourly_summary
group by 1, 2, 3, 4, 5, 6
