{{ config(materialized='view') }}

with source as (
    select * from {{ source('openaq', 'raw_openaq_parameters') }}
),

deduplicated as (
    select *
    from (
        select *,
               row_number() over (
                   partition by parameter_id
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
        
        -- Parameter info
        parameter_id,
        {{ standardize_pollutant_name('name') }} as parameter_name,
        name as parameter_name_original,
        display_name,
        
        -- Units (standardize)
        case
            when lower(units) in ('µg/m³', 'ug/m3', 'ug/m³', 'micrograms per cubic meter') then 'µg/m³'
            when lower(units) in ('ppm', 'parts per million') then 'ppm'
            when lower(units) in ('ppb', 'parts per billion') then 'ppb'
            else units
        end as units,
        units as units_original,
        
        description,
        
        -- Parameter key (standardized parameter name)
        {{ standardize_pollutant_name('name') }} as parameter_key,
        
        raw_payload
        
    from deduplicated
)

select * from transformed

