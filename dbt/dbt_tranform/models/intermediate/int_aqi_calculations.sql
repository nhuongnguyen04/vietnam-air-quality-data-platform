{{ config(materialized='view') }}

-- AQI calculation using US EPA piecewise breakpoint formula via macros.
--
-- The old version used a generic single-bracket formula seeded from pollutants.csv
-- (which only had 1 breakpoint row per pollutant). For PM2.5 = 84 µg/m³ this
-- yielded AQI 350 instead of the correct ~150 because:
--   WRONG: ((84 - 0) / (12.0 - 0)) × (50 - 0) + 0 = 350
--   RIGHT: 84 falls in the 55.5-150.4 bracket → ~150
--
-- This version calls the pollutant-specific macros from macros/calculate_aqi.sql.
-- Calibration factor is applied before AQI computation via LEFT JOIN.
-- CO unit conversion (mg/m³ → ppm) is done in stg_aqiin__measurements.
--
-- NOTE: All columns must be explicitly aliased to avoid ambiguity between
--       the 'unit' column in stg_pollutants__parameters and m.unit from
--       int_unified__measurements.
with calibration as (
    select source, parameter, calibration_factor
    from {{ ref('stg_source_calibration') }}
),
measurements as (
    select
        m.source                                       AS data_source,
        m.station_id                                   AS station_id,
        m.timestamp_utc                               AS timestamp_utc,
        m.parameter                                   AS param_name,
        m.value                                       AS raw_value,
        m.unit                                        AS measurement_unit,
        m.quality_flag                                AS quality_flag,
        m.aqi_reported                                AS aqi_reported,
        m.ingest_time                                 AS ingest_time,
        coalesce(c.calibration_factor, 1.0) * m.value AS calibrated_value
    from {{ ref('int_unified__measurements') }} m
    left join calibration c
        on c.source = m.source and c.parameter = m.parameter
),
calculated as (
    select
        data_source,
        station_id,
        toStartOfHour(timestamp_utc)                   AS hour_key,
        timestamp_utc,
        param_name                                      AS parameter,
        raw_value,
        measurement_unit                                AS unit,
        quality_flag,
        aqi_reported,
        ingest_time,
        calibrated_value,

        -- Call the pollutant-specific AQI macro.
        -- Macro expands to ClickHouse CASE WHEN; column refs (param_name, calibrated_value)
        -- are evaluated per row in the table scan — fully valid ClickHouse syntax.
        {{ calculate_aqi('param_name', 'calibrated_value') }} AS aqi_value
    from measurements
),
with_max as (
    select
        data_source,
        station_id,
        hour_key,
        timestamp_utc,
        parameter,
        raw_value                                       AS value,
        unit,
        quality_flag,
        aqi_reported,
        ingest_time,
        calibrated_value                               AS value_for_aqi,
        aqi_value,
        max(aqi_value) over (
            partition by station_id, toStartOfHour(timestamp_utc)
        )                                              AS max_aqi_in_hour,
        row_number() over (
            partition by station_id, toStartOfHour(timestamp_utc)
            order by aqi_value desc nulls last
        )                                              AS pollutant_rank
    from calculated
)
select
    data_source                                              AS source,
    station_id,
    timestamp_utc,
    parameter,
    value,
    unit,
    quality_flag,
    aqi_reported,
    aqi_value,
    {{ get_aqi_category('aqi_value') }}                     AS aqi_category,
    if(pollutant_rank = 1, true, false)                     AS is_dominant_pollutant,
    'epa_piecewise'                                          AS aqi_calculation_method,
    ingest_time
from with_max
