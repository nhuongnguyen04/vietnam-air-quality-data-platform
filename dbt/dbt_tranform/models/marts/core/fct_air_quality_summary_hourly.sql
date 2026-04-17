{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, ward_code, datetime_hour)',
    order_by='(province, date, assumeNotNull(ward_code))',
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
        ward_code,
        region_3,
        region_8,
        source,
        source_weight,
        ingest_time,
        
        -- Overall AQIs
        max(aqi_us) as final_aqi_us,
        max(aqi_vn) as final_aqi_vn,
        
        -- Concentrations
        avgIf(value, parameter = 'pm25') as pm25_value,
        avgIf(value, parameter = 'pm10') as pm10_value,
        avgIf(value, parameter = 'co')   as co_value,
        avgIf(value, parameter = 'no2')  as no2_value,
        avgIf(value, parameter = 'so2')  as so2_value,
        avgIf(value, parameter = 'o3')   as o3_value,

        -- Sub-AQI Indices (Vietnam Standard)
        avgIf(aqi_vn, parameter = 'pm25') as pm25_aqi,
        avgIf(aqi_vn, parameter = 'pm10') as pm10_aqi,
        avgIf(aqi_vn, parameter = 'co')   as co_aqi,
        avgIf(aqi_vn, parameter = 'no2')  as no2_aqi,
        avgIf(aqi_vn, parameter = 'so2')  as so2_aqi,
        avgIf(aqi_vn, parameter = 'o3')   as o3_aqi,
        
        -- Dominant Pollutants
        argMax(parameter, aqi_us) as dominant_pollutant_us,
        argMax(parameter, aqi_vn) as dominant_pollutant_vn
        
    from calculations
    where parameter in ('pm25', 'pm10', 'co', 'no2', 'so2', 'o3')
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
)

select * from pivoted
