{{ config(
    materialized='view',
    tags=['pipeline_v2']
) }}

{{ process_ward_daily_to_monthly(ref('int_ow__ward_daily')) }}
