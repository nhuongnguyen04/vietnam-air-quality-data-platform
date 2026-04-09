{{ config(materialized='view') }}

select
    pollutant_key,
    display_name,
    unit,
    toFloat64(epa_aqi_bp_lo) as epa_aqi_bp_lo,
    toFloat64(epa_aqi_bp_hi) as epa_aqi_bp_hi,
    toFloat64(conc_bp_lo) as conc_bp_lo,
    toFloat64(conc_bp_hi) as conc_bp_hi,
    health_effects
from {{ ref('pollutants') }}
