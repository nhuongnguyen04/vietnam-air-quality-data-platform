{{ config(
    materialized='view'
) }}

select
    source,
    health_status,
    count() as source_ward_count,
    round(avg(observation_lag_hours), 2) as avg_lag_hours
from {{ ref('dm_platform_data_health') }}
group by source, health_status
