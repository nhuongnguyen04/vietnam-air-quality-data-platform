{{ config(
    materialized='view'
) }}

select
    *
from {{ ref('dm_air_quality_overview_daily') }}
where confidence_score >= 0.5
  and source_mix in ('observed', 'mixed')
