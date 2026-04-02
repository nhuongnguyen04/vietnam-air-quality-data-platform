{{ config(materialized='view', meta = {"event_time": "timestamp_utc"}) }}

with joined as (
    select
        m.*,
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
        timestamp_utc,
        parameter,
        value,
        unit,
        quality_flag,
        case
            when bp_hi is not null and conc_hi != conc_lo then
                ((value - conc_lo) / (conc_hi - conc_lo)) * (bp_hi - bp_lo) + bp_lo
            else null
        end as aqi_value,
        'epa_canonical' as aqi_calculation_method,
        ingest_time
    from joined
    where parameter in ('pm25', 'pm10', 'o3', 'no2', 'co', 'so2')
),
with_dominant as (
    select
        *,
        argMaxIf(parameter, aqi_value) over (
            partition by station_id, toStartOfHour(timestamp_utc)
        ) as dominant_pollutant
    from calculated
)
select
    source,
    station_id,
    timestamp_utc,
    parameter,
    value,
    unit,
    quality_flag,
    aqi_value,
    {{ get_aqi_category('aqi_value') }} as aqi_category,
    case when parameter = dominant_pollutant then true else false end as is_dominant_pollutant,
    aqi_calculation_method,
    ingest_time
from with_dominant
