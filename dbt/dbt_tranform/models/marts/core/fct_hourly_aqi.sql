{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='timestamp_utc',
    batch_size='hour',
    lookback=1,
    begin='2025-01-01',
    engine='AggregatingMergeTree',
    order_by=['datetime_hour', 'station_id', 'pollutant'],
    partition_by='toYYYYMM(datetime_hour)',
    ttl='datetime_hour + INTERVAL 90 DAY'
) }}

with location_tier as (
    select
        station_id,
        sensor_quality_tier,
        source
    from {{ ref('dim_locations') }}
)
select
    toStartOfHour(m.timestamp_utc)                                    AS datetime_hour,
    m.station_id,
    m.parameter                                                      AS pollutant,
    avgState(m.value)                                                AS avg_value,
    avgState(m.aqi_value)                                            AS avg_aqi,
    countState()                                                      AS measurement_count,
    maxState(m.value)                                                AS max_value,
    minState(m.value)                                                AS min_value,
    countStateIf(m.aqi_value > 150)                                  AS exceedance_count_150,
    countStateIf(m.aqi_value > 200)                                  AS exceedance_count_200,
    countStateIf(m.quality_flag != 'valid')                           AS invalid_count,
    avgState(m.aqi_reported)                                          AS avg_aqi_reported,
    argMaxState(m.parameter, m.aqi_value)                            AS dominant_pollutant_state,
    lt.sensor_quality_tier,
    lt.source
from {{ ref('int_aqi_calculations') }} m
left join location_tier lt on m.station_id = lt.station_id
{% if is_incremental() %}
where toStartOfHour(m.timestamp_utc) >= (
    select max(datetime_hour) from {{ this }}
)
{% endif %}
group by
    toStartOfHour(m.timestamp_utc),
    m.station_id,
    m.parameter,
    lt.sensor_quality_tier,
    lt.source
