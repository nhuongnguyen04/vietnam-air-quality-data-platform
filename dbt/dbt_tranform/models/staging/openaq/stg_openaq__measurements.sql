{{ config(materialized='view') }}

with source as (
    select m.*
    from {{ source('openaq', 'raw_openaq_measurements') }} m
    inner join (
        select distinct location_id
        from {{ source('openaq', 'raw_openaq_locations') }}
        where {{ filter_vietnam_openaq() }}
    ) l on m.location_id = l.location_id
),

deduplicated as (
    select *
    from (
        select *,
               row_number() over (
                   partition by location_id, sensor_id, parameter_id, period_datetime_from_utc, period_datetime_to_utc
                   order by ingest_time desc
               ) as rn
        from source
    )
    where rn = 1
),

transformed as (
    select
        -- Metadata
        source,
        ingest_time,
        ingest_batch_id,
        
        -- Location and sensor info
        location_id,
        sensor_id,
        parameter_id,
        
        -- Measurement value (already Float32)
        value,
        
        -- Period information
        period_label,
        period_interval,
        period_datetime_from_utc,
        {{ parse_iso_timestamp('period_datetime_from_local') }} as period_datetime_from_local,
        period_datetime_to_utc,
        {{ parse_iso_timestamp('period_datetime_to_local') }} as period_datetime_to_local,
        
        -- Coverage information
        coverage_expected_count,
        coverage_expected_interval,
        coverage_observed_count,
        coverage_observed_interval,
        coverage_percent_complete,
        coverage_percent_coverage,
        coverage_datetime_from_utc,
        coverage_datetime_to_utc,
        
        -- Flags
        flag_has_flags,
        
        -- Coordinates
        latitude,
        longitude,
        
        -- Summary
        summary,
        
        -- Flags
        true as is_vietnam,
        
        -- Data quality score (based on coverage metrics)
        case
            when coverage_percent_complete >= 90 then 100
            when coverage_percent_complete >= 75 then 75
            when coverage_percent_complete >= 50 then 50
            when coverage_percent_complete >= 25 then 25
            else 10
        end as data_quality_score,
        
        raw_payload
        
    from deduplicated
)

select * from transformed

