-- depends_on: {{ ref('fct_air_quality_ward_level_hourly') }}
{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, datetime_hour)',
    order_by='(province, date)',
    partition_by='toYYYYMM(date)'
) }}

with ward_hourly as (
    select * from {{ ref('fct_air_quality_ward_level_hourly') }}
    {% if is_incremental() %}
    where datetime_hour >= (select max(datetime_hour) - interval 1 day from {{ this }})
    {% endif %}
),

province_hourly as (
    select
        datetime_hour,
        date,
        province,
        region_3,
        region_8,
        
        -- Average of districts in the province
        avg(hourly_avg_aqi_us) as prov_avg_aqi_us,
        avg(hourly_avg_aqi_vn) as prov_avg_aqi_vn,
        
        -- Concentrations
        avg(pm25_hourly_avg) as pm25_prov_avg,
        avg(pm10_hourly_avg) as pm10_prov_avg,
        avg(co_hourly_avg)   as co_prov_avg,
        avg(no2_hourly_avg)  as no2_prov_avg,
        avg(so2_hourly_avg)  as so2_prov_avg,
        avg(o3_hourly_avg)   as o3_prov_avg,

        -- Sub-AQIs
        avg(pm25_hourly_aqi) as pm25_prov_aqi,
        avg(pm10_hourly_aqi) as pm10_prov_aqi,
        avg(co_hourly_aqi)   as co_prov_aqi,
        avg(no2_hourly_aqi)  as no2_prov_aqi,
        avg(so2_hourly_aqi)  as so2_prov_aqi,
        avg(o3_hourly_aqi)   as o3_prov_aqi,
        
        max(last_ingested_at) as last_ingested_at
        
    from ward_hourly
    group by
        datetime_hour,
        date,
        province,
        region_3,
        region_8
),

final as (
    select
        *,
        -- Provincial main pollutant based on averaged sub-AQIs
        case 
            when pm25_prov_aqi >= pm10_prov_aqi and pm25_prov_aqi >= co_prov_aqi and pm25_prov_aqi >= no2_prov_aqi and pm25_prov_aqi >= so2_prov_aqi and pm25_prov_aqi >= o3_prov_aqi then 'pm25'
            when pm10_prov_aqi >= co_prov_aqi and pm10_prov_aqi >= no2_prov_aqi and pm10_prov_aqi >= so2_prov_aqi and pm10_prov_aqi >= o3_prov_aqi then 'pm10'
            when co_prov_aqi >= no2_prov_aqi and co_prov_aqi >= so2_prov_aqi and co_prov_aqi >= o3_prov_aqi then 'co'
            when no2_prov_aqi >= so2_prov_aqi and no2_prov_aqi >= o3_prov_aqi then 'no2'
            when so2_prov_aqi >= o3_prov_aqi then 'so2'
            else 'o3'
        end as main_pollutant
    from province_hourly
)

select * from final
