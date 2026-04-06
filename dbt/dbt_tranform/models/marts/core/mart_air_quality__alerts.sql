-- depends_on: {{ ref('fact_aqi_alerts_final') }}
{{ config(materialized='view') }}

select * from {{ ref('fact_aqi_alerts_final') }}
