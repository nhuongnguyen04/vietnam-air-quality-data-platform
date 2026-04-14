{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, district, datetime_hour)',
    order_by='(province, date, assumeNotNull(district))',
    partition_by='toYYYYMM(date)'
) }}

with calculations as (
    select * from {{ ref('int_aqi__calculations') }}
    {% if is_incremental() %}
    where ingest_time > (select max(ingest_time) from {{ this }})
    {% endif %}
),

pivoted as (
    select
        toStartOfHour(timestamp_utc) as datetime_hour,
        toDate(timestamp_utc) as date,
        province,
        district,
        region_3,
        region_8,
        source,
        ingest_time,
        
        -- Overall AQIs
        max(aqi_us) as final_aqi_us,
        max(aqi_vn) as final_aqi_vn,
        
        -- Concentrations
        maxIf(value, parameter = 'pm25') as pm25_value,
        maxIf(value, parameter = 'pm10') as pm10_value,
        maxIf(value, parameter = 'co')   as co_value,
        maxIf(value, parameter = 'no2')  as no2_value,
        maxIf(value, parameter = 'so2')  as so2_value,
        maxIf(value, parameter = 'o3')   as o3_value,

        -- Sub-AQI Indices (Vietnam Standard)
        maxIf(aqi_vn, parameter = 'pm25') as pm25_aqi,
        maxIf(aqi_vn, parameter = 'pm10') as pm10_aqi,
        maxIf(aqi_vn, parameter = 'co')   as co_aqi,
        maxIf(aqi_vn, parameter = 'no2')  as no2_aqi,
        maxIf(aqi_vn, parameter = 'so2')  as so2_aqi,
        maxIf(aqi_vn, parameter = 'o3')   as o3_aqi,
        
        -- Dominant Pollutants
        argMax(parameter, aqi_us) as dominant_pollutant_us,
        argMax(parameter, aqi_vn) as dominant_pollutant_vn
        
    from calculations
    where parameter in ('pm25', 'pm10', 'co', 'no2', 'so2', 'o3')
    group by 1, 2, 3, 4, 5, 6, 7, 8
)

select * from pivoted
