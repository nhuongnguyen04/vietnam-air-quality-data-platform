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
        region_3,
        region_8,
        latitude,
        longitude,
        argMax(datetime_hour, datetime_hour) as latest_hour,
        argMax(hourly_avg_aqi_us, datetime_hour) as current_aqi_us,
        argMax(hourly_avg_aqi_vn, datetime_hour) as current_aqi_vn,
        argMax(pm25_hourly_avg, datetime_hour) as pm25,
        argMax(pm10_hourly_avg, datetime_hour) as pm10,
        argMax(main_pollutant, datetime_hour) as main_pollutant,
        
        'consolidated' as data_source,
        argMax(last_ingested_at, datetime_hour) as ingest_time
    from hourly_aqi
    group by 
        province, 
        ward_code,
        region_3,
        region_8,
        latitude,
        longitude
)

select * from latest_records
