{{ config(
    materialized='view',
    tags=['pipeline_v2']
) }}

{{ process_ward_daily_to_province_daily(ref('int_ow__ward_daily')) }}
