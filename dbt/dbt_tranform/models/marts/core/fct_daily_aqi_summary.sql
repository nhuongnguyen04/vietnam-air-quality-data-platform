{{ config(materialized='view') }}

-- Public query layer for fct_daily_aqi_summary_state.
-- Reads binary aggregate state via *Merge() functions and returns readable values.
-- Exposes hourly_count for mart and KPI models.
select
    date,
    station_id,
    round(avgMerge(avg_aqi_state), 2)                               AS avg_aqi,
    round(avgMerge(avg_value_state), 2)                             AS avg_value,
    round(minMerge(min_aqi_state), 2)                               AS min_aqi,
    round(maxMerge(max_aqi_state), 2)                               AS max_aqi,
    sumMerge(hourly_count_state)                                    AS hourly_count,
    sumMerge(exceedance_count_150_state)                            AS exceedance_count_150,
    sumMerge(exceedance_count_200_state)                            AS exceedance_count_200,
    finalizeAggregation(argMaxStateMerge(dominant_pollutant_state))  AS dominant_pollutant,
    any(sensor_quality_tier)                                         AS sensor_quality_tier,
    any(source)                                                      AS source
from {{ ref('fct_daily_aqi_summary_state') }}
group by
    date,
    station_id
