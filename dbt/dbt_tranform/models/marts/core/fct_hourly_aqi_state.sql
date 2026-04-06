-- depends_on: {{ ref('dim_locations') }}
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='sync_all_columns',
    engine='AggregatingMergeTree',
    order_by=['datetime_hour', 'station_id', 'pollutant'],
    partition_by='toYYYYMM(datetime_hour)',
    ttl='datetime_hour + INTERVAL 90 DAY'
) }}

-- Internal storage table: stores binary aggregate state via *State() functions.
-- NOT intended for direct SELECT -- use fct_hourly_aqi view instead.
-- NULL values in int_aqi_calculations are excluded from aggregate functions.
-- Uses ifNull(..., 0) for Nullable columns in aggregates to avoid CAST(NULL, Float64) error.
with location_attrs as (
    select station_id,
           any(sensor_quality_tier) as sensor_quality_tier,
           any(source) as source
    from {{ ref('dim_locations') }}
    group by station_id
),
source_hint as (
    select station_id, any(source) as source
    from {{ ref('int_aqi_calculations') }}
    group by station_id
)
select
    CAST(toStartOfHour(m.timestamp_utc), 'DateTime')          AS datetime_hour,
    m.station_id                                               AS station_id,
    m.parameter                                                AS pollutant,
    avgState(m.value)                                           AS avg_value,
    avgState(m.aqi_value)                                       AS avg_aqi,
    countState()                                                AS measurement_count,
    maxState(m.value)                                           AS max_value,
    minState(m.value)                                           AS min_value,
    countStateIf(if(m.aqi_value > 150, 1, 0))                 AS exceedance_count_150,
    countStateIf(if(m.aqi_value > 200, 1, 0))                 AS exceedance_count_200,
    countStateIf(if(m.quality_flag != 'valid', 1, 0))         AS invalid_count,
    -- aqi_reported is Nullable(Int32): ifNull avoids CAST(NULL, Float64) error
    avgState(CAST(ifNull(m.aqi_reported, 0), 'Float64'))       AS avg_aqi_reported,
    argMaxState(m.parameter, m.aqi_value)                     AS dominant_pollutant_state,
    ifNull(la.sensor_quality_tier, sh.source)               AS sensor_quality_tier,
    ifNull(la.source, sh.source)                               AS source
from {{ ref('int_aqi_calculations') }} m
left join location_attrs la on m.station_id = la.station_id
left join source_hint sh on m.station_id = sh.station_id
where
    m.timestamp_utc IS NOT NULL
    {% if is_incremental() %}
    and CAST(toStartOfHour(m.timestamp_utc), 'DateTime') >= (
        select max(datetime_hour) - INTERVAL 2 HOUR
        from {{ this }}
    )
    {% endif %}
group by
    CAST(toStartOfHour(m.timestamp_utc), 'DateTime'),
    m.station_id,
    m.parameter,
    ifNull(la.sensor_quality_tier, sh.source),
    ifNull(la.source, sh.source)
