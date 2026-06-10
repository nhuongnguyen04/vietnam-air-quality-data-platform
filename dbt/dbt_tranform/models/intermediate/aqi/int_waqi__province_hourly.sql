{{ config(
    materialized='view',
    tags=['pipeline_v2']
) }}

{{ process_ward_hourly_to_province_hourly(ref('int_waqi__ward_hourly')) }}
