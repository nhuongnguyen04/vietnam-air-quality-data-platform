{{ config(materialized='view') }}

select
    datetime_hour,
    station_id,
    pollutant,
    round(avgMerge(avg_value), 2)                                    AS avg_value,
    round(avgMerge(avg_aqi), 2)                                      AS normalized_aqi,
    countMerge(measurement_count)                                     AS measurement_count,
    round(maxMerge(max_value), 2)                                     AS max_value,
    round(minMerge(min_value), 2)                                     AS min_value,
    countMerge(exceedance_count_150)                                   AS exceedance_count_150,
    countMerge(exceedance_count_200)                                   AS exceedance_count_200,
    countMerge(invalid_count)                                         AS invalid_count,
    round(avgMerge(avg_aqi_reported), 2)                              AS avg_aqi_reported,
    finalizeAggregation(dominant_pollutant_state)                     AS dominant_pollutant,
    sensor_quality_tier,
    source
from {{ ref('fct_hourly_aqi') }}
group by
    datetime_hour,
    station_id,
    pollutant,
    sensor_quality_tier,
    source
