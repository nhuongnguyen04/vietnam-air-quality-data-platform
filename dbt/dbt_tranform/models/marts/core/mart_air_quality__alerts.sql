{{ config(materialized='view') }}

select * from {{ ref('fact_aqi_alerts_final') }}
