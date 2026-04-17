{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, ward_code)',
    order_by='(province, assumeNotNull(ward_code))'
) }}

with hourly_aqi as (
    select * from {{ ref('fct_air_quality_ward_level_hourly') }}
    {% if is_incremental() %}
    -- Only process new data to update current status
    where last_ingested_at > (select max(ingest_time) from {{ this }})
    {% endif %}
),

latest_records as (
    select
        province,
        ward_code,
        argMax(datetime_hour, datetime_hour) as latest_hour,
        argMax(hourly_avg_aqi_us, datetime_hour) as current_aqi_us,
        argMax(hourly_avg_aqi_vn, datetime_hour) as current_aqi_vn,
        argMax(pm25_hourly_avg, datetime_hour) as pm25,
        argMax(pm10_hourly_avg, datetime_hour) as pm10,
        
        -- Logic for main pollutant at ward level
        argMax(
            case 
                when pm25_hourly_aqi >= pm10_hourly_aqi and pm25_hourly_aqi >= co_hourly_aqi and pm25_hourly_aqi >= no2_hourly_aqi and pm25_hourly_aqi >= so2_hourly_aqi and pm25_hourly_aqi >= o3_hourly_aqi then 'pm25'
                when pm10_hourly_aqi >= co_hourly_aqi and pm10_hourly_aqi >= no2_hourly_aqi and pm10_hourly_aqi >= so2_hourly_aqi and pm10_hourly_aqi >= o3_hourly_aqi then 'pm10'
                when co_hourly_aqi >= no2_hourly_aqi and co_hourly_aqi >= so2_hourly_aqi and co_hourly_aqi >= o3_hourly_aqi then 'co'
                when no2_hourly_aqi >= so2_hourly_aqi and no2_hourly_aqi >= o3_hourly_aqi then 'no2'
                when so2_hourly_aqi >= o3_hourly_aqi then 'so2'
                else 'o3'
            end, 
            datetime_hour
        ) as main_pollutant,
        
        'consolidated' as data_source,
        argMax(last_ingested_at, datetime_hour) as ingest_time
    from hourly_aqi
    group by province, ward_code
)

select * from latest_records
