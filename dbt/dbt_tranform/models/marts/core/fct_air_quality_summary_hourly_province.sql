{{ config(materialized='view') }}

with hourly_summary as (
    select * from {{ ref('fct_air_quality_summary_hourly') }}
)

select
    datetime_hour,
    date,
    province,
    region_3,
    region_8,
    
    -- Average AQIs across all stations in the province
    round(avg(final_aqi_us), 2) as avg_aqi_us,
    round(avg(final_aqi_vn), 2) as avg_aqi_vn,
    max(final_aqi_us) as max_aqi_us,
    max(final_aqi_vn) as max_aqi_vn,
    
    -- Average Concentrations across all stations
    round(avg(pm25_value), 2) as pm25_avg,
    round(avg(pm10_value), 2) as pm10_avg,
    round(avg(co_value), 2)   as co_avg,
    round(avg(no2_value), 2)  as no2_avg,
    round(avg(so2_value), 2)  as so2_avg,
    round(avg(o3_value), 2)   as o3_avg,
    
    count(distinct district) as district_count
    
from hourly_summary
group by 1, 2, 3, 4, 5
