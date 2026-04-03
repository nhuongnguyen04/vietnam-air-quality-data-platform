{{ config(materialized='view') }}

-- Public query layer for fct_daily_aqi_summary_state.
-- Reads from fct_hourly_aqi (public view) to compute daily summaries.
-- any() picks a representative value for per-station columns (constant within each group).
select
    toDate(datetime_hour)                                    AS date,
    station_id,
    round(avg(normalized_aqi), 2)                           AS avg_aqi,
    round(avg(avg_value), 2)                                 AS avg_value,
    round(min(min_value), 2)                                  AS min_aqi,
    round(max(max_value), 2)                                  AS max_aqi,
    count(*)                                                    AS hourly_count,
    sum(exceedance_count_150)                                   AS exceedance_count_150,
    sum(exceedance_count_200)                                   AS exceedance_count_200,
    any(pollutant)                                             AS dominant_pollutant,
    any(sensor_quality_tier)                                     AS sensor_quality_tier,
    any(source)                                                  AS source
from {{ ref('fct_hourly_aqi') }}
group by
    toDate(datetime_hour),
    station_id
