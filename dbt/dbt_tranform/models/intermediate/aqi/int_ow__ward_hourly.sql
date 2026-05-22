{{ config(
    materialized='view',
    tags=['pipeline_v2']
) }}

{{ process_source_to_hourly(ref('int_openweather__processed')) }}
