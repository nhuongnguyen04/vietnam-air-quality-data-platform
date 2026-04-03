{{ config(
    materialized='table',
    engine='ReplacingMergeTree',
    order_by=['date', 'station_id'],
    partition_by='toYYYYMM(date)'
) }}

-- Reads from fct_daily_aqi_summary_state (AMT) via *Merge() functions.
-- Outer query joins with dim_locations for station metadata.
-- Replaced AggregatingMergeTree with ReplacingMergeTree since
-- fct_daily_aqi_summary_state already handles aggregation internally.
with daily_agg as (
    select
        d.date,
        d.station_id,
        round(avgMerge(d.avg_aqi_state), 2)              AS avg_aqi,
        round(minMerge(d.min_aqi_state), 2)               AS min_aqi,
        round(maxMerge(d.max_aqi_state), 2)               AS max_aqi,
        sumMerge(d.hourly_count_state)                    AS hourly_count,
        sumMerge(d.exceedance_count_150_state)             AS exceedance_count_150,
        sumMerge(d.exceedance_count_200_state)             AS exceedance_count_200,
        argMaxMerge(d.dominant_pollutant_state)            AS dominant_pollutant
    from {{ ref('fct_daily_aqi_summary_state') }} d
    group by d.date, d.station_id
)
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
    d.avg_aqi,
    d.min_aqi,
    d.max_aqi,
    d.hourly_count,
    d.exceedance_count_150,
    d.exceedance_count_200,
    d.dominant_pollutant
from daily_agg d
inner join {{ ref('dim_locations') }} l on d.station_id = l.station_id
