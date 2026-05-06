{{ config(
    materialized='view'
) }}

with health as (
    select
        last_seen,
        last_ingested_at,
        observation_lag_hours,
        health_status,
        is_reliable
    from {{ ref('dm_platform_data_health') }}
)

select
    now() as measured_at,
    count() as source_ward_count,
    sum(is_reliable) as reliable_count,
    round(100 * sum(is_reliable) / nullIf(count(), 0), 1) as reliable_pct,
    max(last_seen) as latest_reading_at,
    dateDiff('hour', max(last_seen), now()) as latest_lag_hours,
    max(last_ingested_at) as latest_ingested_at,
    dateDiff('hour', max(last_ingested_at), now()) as latest_ingest_lag_hours,
    round(avgIf(observation_lag_hours, health_status != 'Offline'), 2) as active_avg_lag_hours,
    round(avg(observation_lag_hours), 2) as all_avg_lag_hours,
    countIf(health_status = 'Fresh') as fresh_count,
    countIf(health_status = 'Delayed') as delayed_count,
    countIf(health_status = 'Stale') as stale_count,
    countIf(health_status = 'Offline') as offline_count,
    countIf(health_status in ('Stale', 'Offline')) as attention_count,
    max(observation_lag_hours) as worst_lag_hours
from health
