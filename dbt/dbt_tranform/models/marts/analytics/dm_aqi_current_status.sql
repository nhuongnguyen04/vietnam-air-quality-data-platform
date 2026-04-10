{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, district)',
    order_by='(province, assumeNotNull(district))'
) }}

with hourly_data as (
    select * from {{ ref('fct_air_quality_summary_hourly') }}
    {% if is_incremental() %}
    -- Only process new data to update current status
    where ingest_time > (select max(ingest_time) from {{ this }})
    {% endif %}
),

latest_records as (
    select
        province,
        district,
        argMax(datetime_hour, datetime_hour) as latest_hour,
        argMax(final_aqi_us, datetime_hour) as current_aqi_us,
        argMax(final_aqi_vn, datetime_hour) as current_aqi_vn,
        argMax(pm25_value, datetime_hour) as pm25,
        argMax(pm10_value, datetime_hour) as pm10,
        argMax(dominant_pollutant_us, datetime_hour) as main_pollutant,
        argMax(source, datetime_hour) as data_source,
        argMax(ingest_time, datetime_hour) as ingest_time
    from hourly_data
    group by province, district
)

select * from latest_records
