-- depends_on: {{ ref('fct_hourly_aqi_state') }}
{{ config(materialized='view') }}

-- Public query layer for fct_hourly_aqi_state.
-- Reads binary aggregate state via *Merge() functions and returns readable values.
-- Explicit GROUP BY required: ClickHouse implicit GROUP BY drops columns
-- not referenced in aggregate functions. Must include sensor_quality_tier
-- and source explicitly.
select
    datetime_hour,
    station_id,
    pollutant,
    round(avgMerge(avg_value), 2)                                   AS avg_value,
    round(avgMerge(avg_aqi), 2)                                      AS normalized_aqi,
    countMerge(measurement_count)                                    AS measurement_count,
    round(maxMerge(max_value), 2)                                    AS max_value,
    round(minMerge(min_value), 2)                                    AS min_value,
    countMerge(exceedance_count_150)                                 AS exceedance_count_150,
    countMerge(exceedance_count_200)                                 AS exceedance_count_200,
    countMerge(invalid_count)                                        AS invalid_count,
    round(avgMerge(avg_aqi_reported), 2)                             AS avg_aqi_reported,
    finalizeAggregation(argMaxStateMerge(dominant_pollutant_state))  AS dominant_pollutant,
    any(sensor_quality_tier)                                         AS sensor_quality_tier,
    any(source)                                                      AS source
from {{ ref('fct_hourly_aqi_state') }}
group by
    datetime_hour,
    station_id,
    pollutant
