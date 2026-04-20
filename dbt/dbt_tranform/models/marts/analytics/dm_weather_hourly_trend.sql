{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, ward_code, datetime_hour)',
    order_by='(province, date, assumeNotNull(ward_code))',
    partition_by='toYYYYMM(date)'
) }}

with unified as (
    select
        datetime_hour,
        date,
        province,
        ward_code,
        region_3,
        region_8,
        pm25,
        pm10,
        wind_speed,
        humidity,
        wind_direction,
        temperature,
        weather_influence_pct,
        stagnant_air_probability
    from {{ ref('fct_aqi_weather_traffic_unified') }}
    {% if is_incremental() %}
    where datetime_hour >= (select max(datetime_hour) - interval 1 day from {{ this }})
    {% endif %}
)

select * from unified
