{{ config(
    materialized='view'
) }}

with station_stats as (
    select
        province,
        region_3,
        region_8,
        ward_code,
        source,
        max(datetime_hour) as last_seen,
        max(ingest_time) as last_ingested_at,
        max(raw_loaded_at) as last_loaded_at,
        dateDiff('hour', max(datetime_hour), now()) as observation_lag_hours,
        dateDiff('hour', max(raw_loaded_at), now()) as ingest_lag_hours
    from {{ ref('fct_air_quality_summary_hourly') }}
    group by province, region_3, region_8, ward_code, source
)

select
    *,
    observation_lag_hours as lag_hours,
    case
        when observation_lag_hours <= 1 then 'Fresh'
        when observation_lag_hours <= 3 then 'Delayed'
        when observation_lag_hours <= 24 then 'Stale'
        else 'Offline'
    end as health_status,
    if(observation_lag_hours <= 3, true, false) as is_reliable
from station_stats
