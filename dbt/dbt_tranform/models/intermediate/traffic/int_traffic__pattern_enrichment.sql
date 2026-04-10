{{ config(
    materialized='view',
    schema=var('intermediate_schema', 'intermediate')
) }}

-- Logic has moved to Python modeling job: calculate_hourly_traffic.py
-- This model is now a pass-through to maintain downstream mart compatibility
SELECT
    station_name,
    latitude,
    longitude,
    hour_utc,
    congestion_ratio as estimated_congestion_ratio,
    data_quality_flag,
    now() as dbt_updated_at
FROM {{ ref('stg_tomtom__traffic') }}
