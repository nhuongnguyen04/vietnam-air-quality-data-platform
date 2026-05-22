{{ config(
    materialized='view',
    tags=['pipeline_v2']
) }}

{{ process_ward_daily_to_province_daily(ref('int_ow__ward_daily'), source_mix='modeled', confidence_score=0.5, confidence_level='medium') }}
