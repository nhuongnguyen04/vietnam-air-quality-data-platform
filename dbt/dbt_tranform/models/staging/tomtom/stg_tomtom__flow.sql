{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(ward_code, timestamp_utc, parameter)',
    order_by='(province_name, timestamp_utc, ward_code)',
    partition_by='toYYYYMM(timestamp_utc)',
    query_settings={
        'max_bytes_before_external_sort': 100000000,
        'max_bytes_before_external_group_by': 100000000
    }
) }}

WITH incremental_source AS (
    SELECT
        source,
        traffic_source,
        leftPad(toString(toInt64(toFloat64(assumeNotNull(ward_code)))), 5, '0') as ward_code,
        ward_name,
        province_name,
        latitude,
        longitude,
        toStartOfHour(timestamp_utc) as hourly_timestamp,
        current_travel_time,
        free_flow_travel_time,
        current_speed,
        free_flow_speed,
        confidence,
        ingest_time
    FROM {{ source('tomtom', 'raw_tomtom_traffic') }}
    {% if is_incremental() %}
    WHERE ingest_time >= (
        SELECT max(ingest_time) - interval 24 hour
        FROM {{ this }}
    )
    {% endif %}
),

calculated_ratios AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            "concat('source:', source)",
            "concat('ward_code:', ward_code)",
            "concat('hourly_timestamp:', toString(hourly_timestamp))",
            "concat('parameter:', 'congestion_ratio')"
        ]) }} as dedup_key,
        source,
        traffic_source,
        ward_code,
        ward_name,
        province_name,
        latitude,
        longitude,
        hourly_timestamp,
        current_travel_time,
        free_flow_travel_time,
        current_speed,
        free_flow_speed,
        confidence,
        ingest_time,
        -- Formula: (current travel time - free flow travel time) / free flow travel time
        -- Fallback: (free flow speed / current speed) - 1
        CASE
            WHEN free_flow_travel_time > 0 AND current_travel_time > 0 
                THEN (CAST(current_travel_time, 'Float64') - CAST(free_flow_travel_time, 'Float64')) / CAST(free_flow_travel_time, 'Float64')
            WHEN free_flow_speed > 0 AND current_speed > 0
                THEN (CAST(free_flow_speed, 'Float64') / CAST(current_speed, 'Float64')) - 1
            ELSE 0
        END as raw_congestion_ratio
    FROM incremental_source
),

hourly_aggregated AS (
    SELECT
        dedup_key,
        argMax(source, ingest_time) as source,
        argMax(ward_code, ingest_time) as ward_code,
        argMax(hourly_timestamp, ingest_time) as hourly_timestamp,
        -- Use argMax to get the coordinates associated with the most recent sample
        argMax(latitude, ingest_time) as latitude,
        argMax(longitude, ingest_time) as longitude,
        avg(raw_congestion_ratio) as avg_congestion_ratio,
        avg(confidence) as avg_confidence,
        argMax(traffic_source, ingest_time) as primary_traffic_source,
        argMax(ward_name, ingest_time) as ward_name,
        argMax(province_name, ingest_time) as province_name,
        max(ingest_time) as max_ingest_time
    FROM calculated_ratios
    GROUP BY dedup_key
)

SELECT
    dedup_key,
    source,
    ward_code,
    ward_name,
    province_name,
    latitude,
    longitude,
    hourly_timestamp as timestamp_utc,
    clamp(avg_congestion_ratio, 0, 5) as value,
    'congestion_ratio' as parameter,
    primary_traffic_source as traffic_source,
    avg_confidence as quality_flag,
    max_ingest_time as ingest_time,
    now() as dbt_updated_at
FROM hourly_aggregated
