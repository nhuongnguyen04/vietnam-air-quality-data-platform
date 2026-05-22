{{ config(
    materialized='view',
    tags=['pipeline_v2']
) }}

{{ process_ward_monthly_to_province_monthly(ref('int_ow__ward_monthly'), source_mix='modeled', confidence_score=0.5, confidence_level='medium') }}
