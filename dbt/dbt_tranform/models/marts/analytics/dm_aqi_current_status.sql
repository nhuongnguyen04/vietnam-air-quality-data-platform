{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, district)',
    order_by='(province, assumeNotNull(district))'
) }}

with hourly_data as (
    select * from {{ ref('fct_air_quality_district_level_hourly') }}
    {% if is_incremental() %}
    -- Only process new data to update current status
    where last_ingested_at > (select max(ingest_time) from {{ this }})
    {% endif %}
),

latest_records as (
    select
        province,
        district,
        argMax(datetime_hour, datetime_hour) as latest_hour,
        argMax(avg_aqi_us, datetime_hour) as current_aqi_us,
        argMax(avg_aqi_vn, datetime_hour) as current_aqi_vn,
        argMax(pm25_value, datetime_hour) as pm25,
        argMax(pm10_value, datetime_hour) as pm10,
        
        -- Logic for main pollutant at district level
        argMax(
            case 
                when pm25_aqi >= pm10_aqi and pm25_aqi >= co_aqi and pm25_aqi >= no2_aqi and pm25_aqi >= so2_aqi and pm25_aqi >= o3_aqi then 'pm25'
                when pm10_aqi >= co_aqi and pm10_aqi >= no2_aqi and pm10_aqi >= so2_aqi and pm10_aqi >= o3_aqi then 'pm10'
                when co_aqi >= no2_aqi and co_aqi >= so2_aqi and co_aqi >= o3_aqi then 'co'
                when no2_aqi >= so2_aqi and no2_aqi >= o3_aqi then 'no2'
                when so2_aqi >= o3_aqi then 'so2'
                else 'o3'
            end, 
            datetime_hour
        ) as main_pollutant,
        
        'consolidated' as data_source,
        argMax(last_ingested_at, datetime_hour) as ingest_time
    from hourly_data
    group by province, district
)

select * from latest_records
