-- depends_on: {{ ref('fct_hourly_aqi') }}
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='sync_all_columns',
    engine='AggregatingMergeTree',
    order_by=['date', 'station_id'],
    partition_by='toYYYYMM(date)'
) }}

-- Internal storage table: stores binary aggregate state via *State() functions.
-- NOT intended for direct SELECT -- use fct_daily_aqi_summary view instead.
-- Reads from fct_hourly_aqi (public view with merged readable values).
-- assumeNotNull() on all aggregate inputs to prevent nullable aggregate type mismatch.
{% if is_incremental() %}
{% set min_date = "toDate(datetime_hour) >= (SELECT max(date) - INTERVAL 3 DAY FROM " ~ this ~ ")" %}
{% else %}
{% set min_date = "1=1" %}
{% endif %}

WITH hourly_station_aqi AS (
    -- First, find the max AQI across all pollutants for each station and hour.
    -- This represents the "station's AQI" at that hour.
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
    WHERE {{ min_date }}
    GROUP BY
        datetime_hour,
        station_id
),
daily_agg AS (
    SELECT
        toDate(datetime_hour)                    AS date,
        station_id,
        avg(hour_aqi)                            AS avg_aqi,
        avg(mean_val)                            AS avg_value,
        min(hour_aqi)                            AS min_aqi,
        max(hour_aqi)                            AS max_aqi,
        sum(measurement_count)                   AS hourly_count,
        sum(exceedance_count_150)                AS exceedance_count_150,
        sum(exceedance_count_200)                AS exceedance_count_200,
        argMax(dominant_pollutant, hour_aqi)     AS dominant_pollutant,
        sensor_quality_tier,
        source
    FROM hourly_station_aqi
    GROUP BY
        toDate(datetime_hour),
        station_id,
        sensor_quality_tier,
        source
)
SELECT
    date,
    station_id,
    avgState(assumeNotNull(avg_aqi))                            AS avg_aqi_state,
    avgState(assumeNotNull(avg_value))                          AS avg_value_state,
    minState(assumeNotNull(min_aqi))                            AS min_aqi_state,
    maxState(assumeNotNull(max_aqi))                            AS max_aqi_state,
    sumState(hourly_count)                                      AS hourly_count_state,
    sumState(exceedance_count_150)                              AS exceedance_count_150_state,
    sumState(exceedance_count_200)                              AS exceedance_count_200_state,
    argMaxState(assumeNotNull(dominant_pollutant), avg_aqi)   AS dominant_pollutant_state,
    sensor_quality_tier,
    source
FROM daily_agg
GROUP BY
    date,
    station_id,
    sensor_quality_tier,
    source
