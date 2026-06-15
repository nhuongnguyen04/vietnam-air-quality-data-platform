{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree(version)',
    incremental_strategy='append',
    unique_key='(timestamp_utc, latitude_rounded, longitude_rounded, parameter)',
    order_by='(timestamp_utc, latitude_rounded, longitude_rounded, parameter)',
    partition_by='toYYYYMM(timestamp_utc)',
    tags=['pipeline_v2'],
    query_settings={
        'max_threads': 2
    }
) }}

with observed_stations as (
    select 
        station_name,
        latitude,
        longitude,
        round(latitude, 4) as latitude_rounded,
        round(longitude, 4) as longitude_rounded,
        ward_code,
        province,
        timestamp_utc,
        parameter,
        value,
        value_us_standard,
        aqi_vn,
        aqi_us,
        aqi_reported,
        quality_flag,
        ingest_time,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at,
        region_3,
        region_8,
        if(source = 'waqi', 2, 1) as priority_score,
        priority_score * 3000000000 + toUnixTimestamp(raw_loaded_at) as version
    from (
        select 'aqiin' as source, * from {{ ref('int_aqiin__processed') }}
        where {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
        union all
        select 'waqi' as source, * from {{ ref('int_waqi__processed') }}
        where {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
    )
),

deduped as (
    select * except (priority_score, rn)
    from (
        select *,
            row_number() over (
                partition by 
                    latitude_rounded, 
                    longitude_rounded, 
                    timestamp_utc, 
                    parameter 
                order by version desc
            ) as rn
        from observed_stations
    )
    where rn = 1
)

select * from deduped

