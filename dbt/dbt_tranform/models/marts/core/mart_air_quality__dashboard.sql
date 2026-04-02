{{ config(
    materialized='table',
    engine='AggregatingMergeTree',
    order_by=['date', 'station_id'],
    partition_by='toYYYYMM(date)'
) }}

select
    d.date,
    d.station_id,
    l.station_name,
    l.latitude,
    l.longitude,
    l.city,
    l.province,
    l.source,
    l.sensor_quality_tier,
    round(avgMerge(d.avg_aqi_state), 2)              AS avg_aqi,
    round(minMerge(d.min_aqi_state), 2)                AS min_aqi,
    round(maxMerge(d.max_aqi_state), 2)               AS max_aqi,
    countMerge(d.hourly_count_state)                   AS hourly_count,
    countMerge(d.exceedance_count_150_state)           AS exceedance_count_150,
    countMerge(d.exceedance_count_200_state)           AS exceedance_count_200,
    finalizeAggregation(d.dominant_pollutant_state)    AS dominant_pollutant
from {{ ref('fct_daily_aqi_summary') }} d
inner join {{ ref('dim_locations') }} l on d.station_id = l.station_id
group by
    d.date,
    d.station_id,
    l.station_name,
    l.latitude,
    l.longitude,
    l.city,
    l.province,
    l.source,
    l.sensor_quality_tier
