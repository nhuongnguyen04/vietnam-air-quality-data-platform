{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, month)',
    order_by='(province, month)',
    partition_by='toYYYYMM(month)'
) }}

with ward_monthly as (
    select * from {{ ref('fct_air_quality_ward_level_monthly') }}
    {% if is_incremental() %}
    where month >= (select max(month) - interval 1 month from {{ this }})
    {% endif %}
),

monthly_agg as (
    select
        month,
        province,
        region_3,
        region_8,
        
        avg(monthly_avg_aqi_us) as prov_monthly_avg_aqi_us,
        max(monthly_max_aqi_us) as prov_monthly_max_aqi_us,
        min(monthly_min_aqi_us) as prov_monthly_min_aqi_us,
        
        avg(monthly_avg_aqi_vn) as prov_monthly_avg_aqi_vn,
        max(monthly_max_aqi_vn) as prov_monthly_max_aqi_vn,
        min(monthly_min_aqi_vn) as prov_monthly_min_aqi_vn,
        
        avg(pm25_monthly_avg) as pm25_prov_monthly_avg,
        avg(pm10_monthly_avg) as pm10_prov_monthly_avg,
        avg(co_monthly_avg)   as co_prov_monthly_avg,
        avg(no2_monthly_avg)  as no2_prov_monthly_avg,
        avg(so2_monthly_avg)  as so2_prov_monthly_avg,
        avg(o3_monthly_avg)   as o3_prov_monthly_avg,

        -- Monthly average sub-AQIs for the province
        avg(pm25_monthly_aqi) as pm25_prov_monthly_aqi,
        avg(pm10_monthly_aqi) as pm10_prov_monthly_aqi,
        avg(co_monthly_aqi)   as co_prov_monthly_aqi,
        avg(no2_monthly_aqi)  as no2_prov_monthly_aqi,
        avg(so2_monthly_aqi)  as so2_prov_monthly_aqi,
        avg(o3_monthly_aqi)   as o3_prov_monthly_aqi,
        
        sum(samples_count) as total_samples,
        max(last_ingested_at) as last_ingested_at
        
    from ward_monthly
    group by 1, 2, 3, 4
),

final as (
    select
        *,
        -- Provincial monthly main pollutant
        case 
            when pm25_prov_monthly_aqi >= pm10_prov_monthly_aqi and pm25_prov_monthly_aqi >= co_prov_monthly_aqi and pm25_prov_monthly_aqi >= no2_prov_monthly_aqi and pm25_prov_monthly_aqi >= so2_prov_monthly_aqi and pm25_prov_monthly_aqi >= o3_prov_monthly_aqi then 'pm25'
            when pm10_prov_monthly_aqi >= co_prov_monthly_aqi and pm10_prov_monthly_aqi >= no2_prov_monthly_aqi and pm10_prov_monthly_aqi >= so2_prov_monthly_aqi and pm10_prov_monthly_aqi >= o3_prov_monthly_aqi then 'pm10'
            when co_prov_monthly_aqi >= no2_prov_monthly_aqi and co_prov_monthly_aqi >= so2_prov_monthly_aqi and co_prov_monthly_aqi >= o3_prov_monthly_aqi then 'co'
            when no2_prov_monthly_aqi >= so2_prov_monthly_aqi and no2_prov_monthly_aqi >= so2_prov_monthly_aqi and no2_prov_monthly_aqi >= o3_prov_monthly_aqi then 'no2'
            when so2_prov_monthly_aqi >= o3_prov_monthly_aqi then 'so2'
            else 'o3'
        end as main_pollutant
    from monthly_agg
)

select * from final
