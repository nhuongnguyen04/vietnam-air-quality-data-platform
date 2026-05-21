{{ config(
    materialized='view'
) }}

select
    *
from {{ ref('dm_air_quality_overview_daily') }}
where source_mix = 'modeled'
   or confidence_score < 0.5
