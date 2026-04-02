{{ config(
    materialized='table',
    engine='ReplacingMergeTree',
    order_by=['pollutant_key']
) }}

select
    pollutant_key,
    display_name,
    unit,
    epa_aqi_bp_lo,
    epa_aqi_bp_hi,
    conc_bp_lo,
    conc_bp_hi,
    health_effects
from {{ ref('stg_pollutants__parameters') }}
