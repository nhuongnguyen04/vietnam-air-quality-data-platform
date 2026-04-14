{{ config(
    materialized='view'
) }}

WITH raw_pop AS (
    SELECT * FROM {{ ref('population_density_2026') }}
),

normalization AS (
    -- Use the project-standard normalization seed
    SELECT * FROM {{ ref('province_normalization') }}
),

cleaned AS (
    SELECT
        -- Map raw name to the standardized target_name (with accents)
        -- In ClickHouse, LEFT JOIN on non-nullable columns returns empty strings for missing matches, not NULL.
        if(n.target_name != '', n.target_name, r."Địa phương") as standardized_location_name,
        r."Địa phương" as original_location_name,
        CAST(r."2026 Dân số trung bình (Nghìn người)", 'Float64') as population_thousand,
        CAST(r."2026 Diện tích(Km2)(*)", 'Float64') as area_km2,
        CAST(r."2026 Mật độ dân số (Người/km2)", 'Float64') as density_per_km2
    FROM raw_pop r
    LEFT JOIN normalization n ON r."Địa phương" = n.raw_name
    WHERE r."Địa phương" NOT IN ('Cả Nước', 'Đồng bằng sông Hồng', 'Trung du và miền núi phía Bắc', 'Bắc Trung Bộ và Duyên hải miền Trung', 'Tây Nguyên', 'Đông Nam Bộ', 'Đồng bằng sông Cửu Long')
)

SELECT
    standardized_location_name as location_name,
    original_location_name,
    population_thousand * 1000 as total_population,
    area_km2,
    density_per_km2
FROM cleaned
