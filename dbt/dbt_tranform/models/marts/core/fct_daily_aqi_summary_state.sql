-- depends_on: {{ ref('fct_hourly_aqi') }}
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
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

WITH daily_agg AS (
    SELECT
        toDate(datetime_hour)                    AS date,
        station_id,
        avg(normalized_aqi)                      AS avg_aqi,
        avg(avg_value)                           AS avg_value,
        min(min_value)                           AS min_aqi,
        max(max_value)                           AS max_aqi,
        sum(measurement_count)                   AS hourly_count,
        sum(exceedance_count_150)                AS exceedance_count_150,
        sum(exceedance_count_200)                AS exceedance_count_200,
        argMax(pollutant, normalized_aqi)        AS dominant_pollutant,
        sensor_quality_tier,
        source
    FROM {{ ref('fct_hourly_aqi') }}
    WHERE {{ min_date }}
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
