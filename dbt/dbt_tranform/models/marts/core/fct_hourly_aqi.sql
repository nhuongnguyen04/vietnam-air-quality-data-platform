{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='sync_all_columns',
    engine='AggregatingMergeTree',
    order_by=['datetime_hour', 'station_id', 'pollutant'],
    partition_by='toYYYYMM(datetime_hour)',
    ttl='datetime_hour + INTERVAL 90 DAY'
) }}

-- dim_locations provides sensor_quality_tier and source per station_id
-- Use COALESCE on dim_locations LEFT JOIN so unmatched stations still get processed
-- Nullable columns from int_aqi_calculations are wrapped with ifNull for aggregate functions
with location_tier as (
    select
        station_id,
        sensor_quality_tier,
        source
    from {{ ref('dim_locations') }}
),
source_hint as (
    select distinct
        station_id,
        source
    from {{ ref('int_aqi_calculations') }}
)
select
    CAST(toStartOfHour(m.timestamp_utc), 'DateTime')            AS datetime_hour,
    m.station_id                                                AS station_id,
    m.parameter                                                 AS pollutant,
    avgState(ifNull(m.value, 0.0))                              AS avg_value,
    avgState(ifNull(m.aqi_value, 0.0))                         AS avg_aqi,
    countState()                                                 AS measurement_count,
    maxState(ifNull(m.value, 0.0))                             AS max_value,
    minState(ifNull(m.value, 0.0))                             AS min_value,
    countStateIf(if(ifNull(m.aqi_value, 0.0) > 150, 1, 0))    AS exceedance_count_150,
    countStateIf(if(ifNull(m.aqi_value, 0.0) > 200, 1, 0))    AS exceedance_count_200,
    countStateIf(if(m.quality_flag != 'valid', 1, 0))           AS invalid_count,
    avgState(CAST(ifNull(m.aqi_reported, 0), 'Float64'))        AS avg_aqi_reported,
    argMaxState(m.parameter, ifNull(m.aqi_value, 0.0))          AS dominant_pollutant_state,
    coalesce(lt.sensor_quality_tier, sh.source)                 AS sensor_quality_tier,
    coalesce(lt.source, sh.source)                               AS source
from {{ ref('int_aqi_calculations') }} m
left join location_tier lt on m.station_id = lt.station_id
left join source_hint sh on m.station_id = sh.station_id
{% if is_incremental() %}
where CAST(toStartOfHour(m.timestamp_utc), 'DateTime') >= (
    select max(datetime_hour) from {{ this }}
)
{% endif %}
group by
    CAST(toStartOfHour(m.timestamp_utc), 'DateTime'),
    m.station_id,
    m.parameter,
    lt.sensor_quality_tier,
    lt.source,
    sh.source
