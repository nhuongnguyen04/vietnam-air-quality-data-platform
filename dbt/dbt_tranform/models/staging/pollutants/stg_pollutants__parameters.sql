{{ config(materialized='view') }}

select
    pollutant_key,
    display_name,
    unit,
    toFloat64(epa_aqi_bp_lo)  AS epa_aqi_bp_lo,
    toFloat64(epa_aqi_bp_hi)  AS epa_aqi_bp_hi,
    toFloat64(conc_bp_lo)      AS conc_bp_lo,
    toFloat64(conc_bp_hi)      AS conc_bp_hi,
    health_effects
from {{ ref('pollutants') }}
