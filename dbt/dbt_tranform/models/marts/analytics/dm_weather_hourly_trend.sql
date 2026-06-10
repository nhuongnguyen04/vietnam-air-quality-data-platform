{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    engine='ReplacingMergeTree',
    unique_key='(province, ward_code, datetime_hour)',
    order_by='(province, date, assumeNotNull(ward_code), datetime_hour)',
    partition_by='toYYYYMM(date)'
) }}

with
{% if is_incremental() %}
affected_hours as (
    select distinct datetime_hour as affected_hour
    from {{ ref('fct_air_quality_ward_level_hourly') }}
    where {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
    
    union distinct
    
    select distinct datetime_hour as affected_hour
    from {{ ref('fct_weather_ward_hourly') }}
    where {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
),
{% endif %}

aqi as (
    select
        datetime_hour,
        date,
        province,
        ward_code,
        region_3,
        region_8,
        pm25_avg as pm25,
        pm10_avg as pm10,
        source_mix,
        confidence_score,
        confidence_level,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at
    from {{ ref('fct_air_quality_ward_level_hourly') }}
    where 1 = 1
    {% if is_incremental() %}
      and datetime_hour in (select affected_hour from affected_hours)
    {% endif %}
),

weather as (
    select
        ward_code,
        datetime_hour,
        coalesce(temperature, province_temperature) as temperature,
        coalesce(humidity, province_humidity) as humidity,
        coalesce(wind_speed, province_wind_speed) as wind_speed,
        coalesce(wind_direction, province_wind_direction) as wind_direction,
        stagnant_air_probability,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at
    from {{ ref('fct_weather_ward_hourly') }}
    where 1 = 1
    {% if is_incremental() %}
      and datetime_hour in (select affected_hour from affected_hours)
    {% endif %}
)

select
    a.datetime_hour,
    a.date,
    assumeNotNull(a.province) as province,
    assumeNotNull(a.ward_code) as ward_code,
    a.region_3,
    a.region_8,
    a.pm25,
    a.pm10,
    cast(w.wind_speed as Nullable(Float64)) as wind_speed,
    cast(w.humidity as Nullable(Float64)) as humidity,
    cast(w.wind_direction as Nullable(Float64)) as wind_direction,
    cast(w.temperature as Nullable(Float64)) as temperature,
    cast(0.0 as Float32) as weather_influence_pct,
    cast(w.stagnant_air_probability as Float32) as stagnant_air_probability,
    a.source_mix,
    a.confidence_score,
    a.confidence_level,
    
    greatest(
        coalesce(a.raw_loaded_at, toDateTime('1970-01-01 00:00:00')),
        coalesce(w.raw_loaded_at, toDateTime('1970-01-01 00:00:00'))
    ) as raw_loaded_at,
    coalesce(w.raw_sync_run_id, a.raw_sync_run_id, '') as raw_sync_run_id,
    greatest(
        coalesce(a.raw_sync_started_at, toDateTime('1970-01-01 00:00:00')),
        coalesce(w.raw_sync_started_at, toDateTime('1970-01-01 00:00:00'))
    ) as raw_sync_started_at
from aqi a
left join weather w 
    on a.ward_code = w.ward_code 
   and a.datetime_hour = w.datetime_hour
