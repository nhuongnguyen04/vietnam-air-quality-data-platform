{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree(raw_loaded_at)',
    incremental_strategy='append',
    unique_key='(station_name, timestamp_utc, parameter)',
    order_by='(province, timestamp_utc, ward_code, station_name, parameter)',
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
        if(source = 'waqi', 1, 2) as priority -- WAQI (1) ưu tiên hơn AQI.in (2)
    from (
        select 'aqiin' as source, * from {{ ref('int_aqiin__processed') }}
        where {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
        union all
        select 'waqi' as source, * from {{ ref('int_waqi__processed') }}
        where {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
    )
),

deduped as (
    -- Khử trùng lặp cấp trạm theo Tọa độ làm tròn:
    -- Nếu trùng (round(lat, 4), round(lon, 4)) + giờ + chỉ số, chỉ giữ lại WAQI
    select * except (priority, rn)
    from (
        select *,
            row_number() over (
                partition by 
                    round(latitude, 4), 
                    round(longitude, 4), 
                    timestamp_utc, 
                    parameter 
                order by priority asc, raw_loaded_at desc
            ) as rn
        from observed_stations
    )
    where rn = 1
)

select * from deduped
