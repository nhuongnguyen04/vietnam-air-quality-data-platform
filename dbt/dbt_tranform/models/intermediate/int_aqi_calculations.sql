{{ config(materialized='view', meta = {'event_time': 'timestamp_utc'}) }}

with joined as (
    select
        m.source,
        m.station_id,
        m.timestamp_utc,
        m.parameter,
        m.value,
        m.unit,
        m.quality_flag,
        m.aqi_reported,
        m.ingest_time,
        p.epa_aqi_bp_lo  AS bp_lo,
        p.epa_aqi_bp_hi  AS bp_hi,
        p.conc_bp_lo     AS conc_lo,
        p.conc_bp_hi     AS conc_hi
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
                ((value - conc_lo) / (conc_hi - conc_lo)) * (bp_hi - bp_lo) + bp_lo
            else null
        end AS aqi_value
    from joined
    where parameter in ('pm25', 'pm10', 'o3', 'no2', 'co', 'so2')
),
with_max as (
    select
        c.*,
        c.aqi_value,
        -- Dominant pollutant: pollutant with highest AQI value per station-hour
        -- First, find the max AQI per station-hour
        max(c.aqi_value) over (
            partition by c.station_id, toStartOfHour(c.timestamp_utc)
        ) AS max_aqi_in_hour,
        -- Rank pollutants by AQI within each station-hour
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
