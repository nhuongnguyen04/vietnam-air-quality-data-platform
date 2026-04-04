{{ config(materialized='view') }}

-- AQI calculation using EPA canonical formula.
-- Handles unit conversion: OpenWeather CO is in µg/m³, must be converted to ppm for EPA breakpoints.
-- EPA CO breakpoints are in ppm: 0-4.4 (AQI 0-50), 4.4-9.4 (51-100), 9.4-12.4 (101-150),
--   12.4-15.4 (151-200), 15.4-30.4 (201-300), 30.4-50.4 (301-400), 50.4-100 (401-500).
-- Conversion: 1 ppm CO = 1000 µg/m³.
with joined as (
    select
        m.source,
        m.station_id,
        m.timestamp_utc,
        m.parameter,
        m.value,                                       -- raw value (µg/m³ for CO)
        m.unit,
        m.quality_flag,
        m.aqi_reported,
        m.ingest_time,
        p.epa_aqi_bp_lo  AS bp_lo,
        p.epa_aqi_bp_hi  AS bp_hi,
        p.conc_bp_lo     AS conc_lo,
        p.conc_bp_hi     AS conc_hi,
        -- Convert µg/m³ to ppm for CO before AQI calculation (breakpoints in ppm)
        if(m.parameter = 'co' and m.unit = 'µg/m³',
           m.value / 1000.0,
           m.value) AS value_for_aqi
    from {{ ref('int_unified__measurements') }} m
    inner join {{ ref('stg_pollutants__parameters') }} p
        on m.parameter = p.pollutant_key
),
calculated as (
    select
        source,
        station_id,
        toStartOfHour(timestamp_utc) AS hour_key,
        timestamp_utc,
        parameter,
        value,
        unit,
        quality_flag,
        aqi_reported,
        ingest_time,
        case
            when bp_hi is not null and conc_hi != conc_lo then
                ((value_for_aqi - conc_lo) / (conc_hi - conc_lo)) * (bp_hi - bp_lo) + bp_lo
            else null
        end AS aqi_value
    from joined
    where parameter in ('pm25', 'pm10', 'o3', 'no2', 'co', 'so2')
),
with_max as (
    select
        c.*,
        c.aqi_value,
        max(c.aqi_value) over (
            partition by c.station_id, toStartOfHour(c.timestamp_utc)
        ) AS max_aqi_in_hour,
        row_number() over (
            partition by c.station_id, toStartOfHour(c.timestamp_utc)
            order by c.aqi_value desc nulls last
        ) AS pollutant_rank
    from calculated c
)
select
    source,
    station_id,
    timestamp_utc,
    parameter,
    value,
    unit,
    quality_flag,
    aqi_reported,
    aqi_value,
    {{ get_aqi_category('aqi_value') }} AS aqi_category,
    if(pollutant_rank = 1, true, false) AS is_dominant_pollutant,
    'epa_canonical'                       AS aqi_calculation_method,
    ingest_time
from with_max
