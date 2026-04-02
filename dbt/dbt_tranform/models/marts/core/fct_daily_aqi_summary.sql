{{ config(
    materialized='table',
    engine='AggregatingMergeTree',
    order_by=['date', 'station_id'],
    partition_by='toYYYYMM(date)'
) }}

select
    toDate(datetime_hour)                                        AS date,
    station_id,
    avgState(normalized_aqi)                                     AS avg_aqi_state,
    avgState(avg_value)                                           AS avg_value_state,
    minState(normalized_aqi)                                      AS min_aqi_state,
    maxState(normalized_aqi)                                      AS max_aqi_state,
    countState()                                                  AS hourly_count_state,
    sumState(exceedance_count_150)                                AS exceedance_count_150_state,
    sumState(exceedance_count_200)                                AS exceedance_count_200_state,
    argMaxState(dominant_pollutant, normalized_aqi)              AS dominant_pollutant_state,
    sensor_quality_tier,
    source
from {{ ref('fct_hourly_aqi_final') }}
group by
    toDate(datetime_hour),
    station_id,
    sensor_quality_tier,
    source
