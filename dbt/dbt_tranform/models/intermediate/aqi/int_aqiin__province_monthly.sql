{{ config(
    materialized='view',
    tags=['pipeline_v2']
) }}

{{ process_ward_monthly_to_province_monthly(ref('int_aqiin__ward_monthly')) }}
