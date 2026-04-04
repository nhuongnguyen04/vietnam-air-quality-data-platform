{{ config(
    materialized='incremental',
    unique_key='station_id || datetime_hour || threshold_breached',
    engine='MergeTree',
    order_by=['station_id', 'datetime_hour', 'threshold_breached']
) }}

with alerts_150 as (
    select
        station_id,
        datetime_hour,
        '150'                                                        AS threshold_breached,
        normalized_aqi,
        dominant_pollutant,
        source,
        sensor_quality_tier,
        now()                                                        AS created_at
    from {{ ref('fct_hourly_aqi') }}
    where normalized_aqi > 150
),
alerts_200 as (
    select
        station_id,
        datetime_hour,
        '200'                                                        AS threshold_breached,
        normalized_aqi,
        dominant_pollutant,
        source,
        sensor_quality_tier,
        now()                                                        AS created_at
    from {{ ref('fct_hourly_aqi') }}
    where normalized_aqi > 200
)
select
    station_id,
    datetime_hour,
    threshold_breached,
    normalized_aqi,
    dominant_pollutant,
    source,
    sensor_quality_tier,
    created_at
from alerts_150
union all
select
    station_id,
    datetime_hour,
    threshold_breached,
    normalized_aqi,
    dominant_pollutant,
    source,
    sensor_quality_tier,
    created_at
from alerts_200
