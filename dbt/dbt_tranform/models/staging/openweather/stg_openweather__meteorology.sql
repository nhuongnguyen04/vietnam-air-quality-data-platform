{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    engine='ReplacingMergeTree(ingest_time)',
    unique_key='(ward_code, timestamp_utc)',
    order_by='(province, timestamp_utc, ward_code)',
    partition_by='toYYYYMM(timestamp_utc)',
    query_settings={
        'max_threads': 1,
        'max_bytes_before_external_sort': 100000000
    }
) }}

with ward_cluster_map as (
    select
        ward_code,
        ward_name,
        province,
        lat as latitude,
        lon as longitude,
        concat(
            'grid_',
            toString(toDecimal64(round(lat / 0.2) * 0.2, 1)),
            '_',
            toString(toDecimal64(round(lon / 0.2) * 0.2, 1))
        ) as weather_cluster
    from {{ ref('stg_core__administrative_units') }}
),

incremental_source as (
    select
        source,
        province_name as province,
        weather_cluster,
        cluster_lat,
        cluster_lon,
        toStartOfHour(timestamp_utc) as timestamp_utc,
        temp,
        temp_min,
        temp_max,
        feels_like,
        humidity,
        pressure,
        visibility,
        wind_speed,
        wind_deg,
        clouds_all,
        ingest_time
    from {{ source('openweather', 'raw_openweather_meteorology') }}
    {% if is_incremental() %}
    where ingest_time >= (
        select max(ingest_time) - interval 6 hour
        from {{ this }}
    )
    {% endif %}
),

mapped_to_wards as (
    select
        s.source,
        m.ward_code as ward_code,
        m.ward_name as ward_name,
        m.province as province,
        m.latitude as latitude,
        m.longitude as longitude,
        s.timestamp_utc,
        s.temp,
        s.temp_min,
        s.temp_max,
        s.feels_like,
        s.humidity,
        s.pressure,
        s.visibility,
        s.wind_speed,
        s.wind_deg,
        s.clouds_all,
        s.ingest_time
    from incremental_source s
    inner join ward_cluster_map m
        on s.weather_cluster = m.weather_cluster
       and s.province = m.province
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            "concat('ward_code:', ward_code)",
            "concat('timestamp_utc:', toString(timestamp_utc))"
        ]) }} as dedup_key,
        source,
        ward_code,
        ward_name,
        province,
        latitude,
        longitude,
        timestamp_utc,
        temp,
        temp_min,
        temp_max,
        feels_like,
        humidity,
        pressure,
        visibility as visibility_meters,
        wind_speed,
        wind_deg,
        clouds_all,
        ingest_time,
        now() as dbt_updated_at
    from mapped_to_wards
    where ward_code != ''
)

select * from final
