{{ config(materialized='table') }}

with station_info as (
    select
        unified_location_id,
        source_system,
        location_name,
        latitude,
        longitude,
        province,
        city,
        location_type,
        current_aqi,
        last_update,
        source_systems
    from {{ ref('int_unified__stations') }}
),

latest_measurements as (
    select
        unified_station_id,
        max(measurement_datetime) as latest_measurement_datetime,
        argMax(aqi_value, measurement_datetime) as current_aqi_from_measurements,
        avg(aqi_value) as avg_aqi_7d,
        avg(aqi_value) as avg_aqi_30d,
        avg(aqi_value) as avg_aqi_90d
    from {{ ref('int_aqi_calculations') }}
    where measurement_datetime >= now() - INTERVAL 90 DAY
    group by unified_station_id
),

aqi_7d as (
    select
        unified_station_id,
        avg(aqi_value) as avg_aqi_7d
    from {{ ref('int_aqi_calculations') }}
    where measurement_datetime >= now() - INTERVAL 7 DAY
    group by unified_station_id
),

aqi_30d as (
    select
        unified_station_id,
        avg(aqi_value) as avg_aqi_30d
    from {{ ref('int_aqi_calculations') }}
    where measurement_datetime >= now() - INTERVAL 30 DAY
    group by unified_station_id
),

aqi_90d as (
    select
        unified_station_id,
        avg(aqi_value) as avg_aqi_90d
    from {{ ref('int_aqi_calculations') }}
    where measurement_datetime >= now() - INTERVAL 90 DAY
    group by unified_station_id
),

station_ranking as (
    select
        unified_station_id,
        avg_aqi_30d,
        row_number() over (order by avg_aqi_30d desc) as pollution_rank
    from aqi_30d
),

data_quality as (
    select
        unified_station_id,
        avg(overall_data_quality_score) as avg_data_quality_score
    from {{ ref('int_data_quality') }}
    where measurement_date >= now() - INTERVAL 30 DAY
    group by unified_station_id
)

select
    s.unified_location_id as unified_station_id,
    s.source_system,
    s.location_name as station_name,
    s.latitude,
    s.longitude,
    s.province,
    s.city,
    s.location_type,
    coalesce(lm.current_aqi_from_measurements, s.current_aqi) as current_aqi,
    a7.avg_aqi_7d as avg_aqi_7d,
    a30.avg_aqi_30d as avg_aqi_30d,
    a90.avg_aqi_90d as avg_aqi_90d,
    sr.pollution_rank,
    coalesce(dq.avg_data_quality_score, 0) as data_quality_score,
    lm.latest_measurement_datetime,
    s.last_update,
    s.source_systems
from station_info s
left join latest_measurements lm on s.unified_location_id = lm.unified_station_id
left join aqi_7d a7 on s.unified_location_id = a7.unified_station_id
left join aqi_30d a30 on s.unified_location_id = a30.unified_station_id
left join aqi_90d a90 on s.unified_location_id = a90.unified_station_id
left join station_ranking sr on s.unified_location_id = sr.unified_station_id
left join data_quality dq on s.unified_location_id = dq.unified_station_id

