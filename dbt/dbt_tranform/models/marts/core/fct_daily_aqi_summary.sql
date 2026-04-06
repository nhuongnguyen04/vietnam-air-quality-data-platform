-- depends_on: {{ ref('fct_hourly_aqi') }}
{{ config(materialized='view') }}

-- Public query layer for fct_daily_aqi_summary_state.
-- Reads from fct_hourly_aqi (public view) to compute daily summaries.
-- any() picks a representative value for per-station columns (constant within each group).
WITH hourly_station_aqi AS (
    -- First, reduce every station's hour to a single AQI (MAX of all its pollutants).
    -- Standard practice: Station AQI = Dominant Pollutant's AQI.
    SELECT
        datetime_hour,
        station_id,
        max(normalized_aqi)                      AS hour_aqi,
        argMax(pollutant, normalized_aqi)        AS dominant_pollutant,
        any(sensor_quality_tier)                 AS sensor_quality_tier,
        any(source)                              AS source,
        sum(measurement_count)                   AS measurement_count,
        sum(exceedance_count_150)                AS exceedance_count_150,
        sum(exceedance_count_200)                AS exceedance_count_200,
        max(max_value)                           AS max_val,
        min(min_value)                           AS min_val,
        avg(avg_value)                           AS mean_val
    FROM {{ ref('fct_hourly_aqi') }}
    GROUP BY
        datetime_hour,
        station_id
)
select
    toDate(datetime_hour)                                    AS date,
    station_id,
    round(avg(hour_aqi), 2)                                 AS avg_aqi,
    round(avg(mean_val), 2)                                  AS avg_value,
    round(min(hour_aqi), 2)                                  AS min_aqi,
    round(max(hour_aqi), 2)                                  AS max_aqi,
    sum(measurement_count)                                   AS total_count,
    sum(exceedance_count_150)                                   AS exceedance_count_150,
    sum(exceedance_count_200)                                   AS exceedance_count_200,
    argMax(dominant_pollutant, hour_aqi)                        AS dominant_pollutant,
    any(sensor_quality_tier)                                     AS sensor_quality_tier,
    any(source)                                                  AS source
from hourly_station_aqi
group by
    toDate(datetime_hour),
    station_id
