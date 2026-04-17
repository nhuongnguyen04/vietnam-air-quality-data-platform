{{ config(
    materialized='view'
) }}

-- Logic has moved to Python modeling job: calculate_hourly_traffic.py
-- This model is now a pass-through to maintain downstream mart compatibility
SELECT
    ward_name as station_name,
    latitude,
    longitude,
    timestamp_utc,
    value as estimated_congestion_ratio,
    quality_flag as data_quality_flag,
    now() as dbt_updated_at
FROM {{ ref('stg_tomtom__flow') }}
