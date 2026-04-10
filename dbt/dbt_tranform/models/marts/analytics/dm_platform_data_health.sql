{{ config(
    materialized='view'
) }}

with station_stats as (
    select
        province,
        district,
        source,
        max(datetime_hour) as last_seen,
        dateDiff('hour', max(datetime_hour), now()) as lag_hours
    from {{ ref('fct_air_quality_summary_hourly') }}
    group by province, district, source
)

select
    *,
    case
        when lag_hours <= 1 then 'Fresh'
        when lag_hours <= 3 then 'Delayed'
        when lag_hours <= 24 then 'Stale'
        else 'Offline'
    end as health_status,
    if(lag_hours <= 3, true, false) as is_reliable
from station_stats
