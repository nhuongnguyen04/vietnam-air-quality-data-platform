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
    where {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
)

select * from unified
