{{ config(materialized='view') }}

select
    date,
    station_id,
    round(avgMerge(avg_aqi_state), 2)                            AS avg_aqi,
    round(avgMerge(avg_value_state), 2)                          AS avg_value,
    round(minMerge(min_aqi_state), 2)                            AS min_aqi,
    round(maxMerge(max_aqi_state), 2)                            AS max_aqi,
    countMerge(hourly_count_state)                                AS hourly_count,
    countMerge(exceedance_count_150_state)                       AS exceedance_count_150,
    countMerge(exceedance_count_200_state)                       AS exceedance_count_200,
    finalizeAggregation(dominant_pollutant_state)                AS dominant_pollutant,
    sensor_quality_tier,
    source
from {{ ref('fct_daily_aqi_summary') }}
group by
    date,
    station_id,
    sensor_quality_tier,
    source
