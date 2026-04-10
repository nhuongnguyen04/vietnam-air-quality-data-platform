{{ config(
    materialized='table'
) }}

WITH raw_pop AS (
    SELECT * FROM {{ ref('population_density_2026') }}
),

cleaned AS (
    SELECT
        "Địa phương" as location_name,
        -- Standardize names to match ClickHouse Title Case without accents (partially)
        -- In a real scenario, we might need a mapping table.
        -- For now, we normalize the most common ones.
        multiIf(
            "Địa phương" = 'Cả Nước', 'All',
            "Địa phương" = 'Hà Nội', 'Ha Noi',
            "Địa phương" = 'TP.Hồ Chí Minh', 'Ho Chi Minh',
            "Địa phương" = 'Hồ Chí Minh', 'Ho Chi Minh',
            "Địa phương" = 'Thành phố Hồ Chí Minh', 'Ho Chi Minh',
            "Địa phương" = 'Đà Nẵng', 'Da Nang',
            "Địa phương" = 'Hải Phòng', 'Hai Phong',
            "Địa phương" = 'Cần Thơ', 'Can Tho',
            "Địa phương" = 'Bà Rịa - Vũng Tàu', 'Ba Ria - Vung Tau',
            "Địa phương" = 'Bắc Giang', 'Bac Giang',
            "Địa phương" = 'Bắc Ninh', 'Bac Ninh',
            "Địa phương" = 'Vĩnh Phúc', 'Vinh Phuc',
            "Địa phương" = 'Hưng Yên', 'Hung Yen',
            "Địa phương" = 'Hải Dương', 'Hai Duong',
            "Địa phương" = 'Quảng Ninh', 'Quang Ninh',
            "Địa phương" = 'Nam Định', 'Nam Dinh',
            "Địa phương" = 'Thái Bình', 'Thai Binh',
            "Địa phương" = 'Ninh Bình', 'Ninh Binh',
            "Địa phương" = 'Hà Nam', 'Ha Nam',
            "Địa phương" = 'Thanh Hóa', 'Thanh Hoa',
            "Địa phương" = 'Nghệ An', 'Nghe An',
            "Địa phương" = 'Hà Tĩnh', 'Ha Tinh',
            "Địa phương" = 'Quảng Bình', 'Quang Binh',
            "Địa phương" = 'Quảng Trị', 'Quang Tri',
            "Địa phương" = 'Thừa Thiên Huế', 'Thua Thien Hue',
            "Địa phương" = 'Thừa Thiên - Huế', 'Thua Thien Hue',
            "Địa phương" = 'Quảng Nam', 'Quang Nam',
            "Địa phương" = 'Quảng Ngãi', 'Quang Ngai',
            "Địa phương" = 'Bình Định', 'Binh Dinh',
            "Địa phương" = 'Phú Yên', 'Phu Yen',
            "Địa phương" = 'Khánh Hòa', 'Khanh Hoa',
            "Địa phương" = 'Ninh Thuận', 'Ninh Thuan',
            "Địa phương" = 'Bình Thuận', 'Binh Thuan',
            "Địa phương" = 'Kon Tum', 'Kon Tum',
            "Địa phương" = 'Gia Lai', 'Gia Lai',
            "Địa phương" = 'Đắk Lắk', 'Dak Lak',
            "Địa phương" = 'Đắk Nông', 'Dak Nong',
            "Địa phương" = 'Lâm Đồng', 'Lam Dong',
            "Địa phương" = 'Bình Phước', 'Binh Phuoc',
            "Địa phương" = 'Bình Dương', 'Binh Duong',
            "Địa phương" = 'Tây Ninh', 'Tay Ninh',
            "Địa phương" = 'Đồng Nai', 'Dong Nai',
            "Địa phương" = 'Long An', 'Long An',
            "Địa phương" = 'Tiền Giang', 'Tien Giang',
            "Địa phương" = 'Bến Tre', 'Ben Tre',
            "Địa phương" = 'Trà Vinh', 'Tra Vinh',
            "Địa phương" = 'Vĩnh Long', 'Vinh Long',
            "Địa phương" = 'Đồng Tháp', 'Dong Thap',
            "Địa phương" = 'An Giang', 'An Giang',
            "Địa phương" = 'Kiên Giang', 'Kien Giang',
            "Địa phương" = 'Hậu Giang', 'Hau Giang',
            "Địa phương" = 'Sóc Trăng', 'Soc Trang',
            "Địa phương" = 'Bạc Liêu', 'Bac Lieu',
            "Địa phương" = 'Cà Mau', 'Ca Mau',
            "Địa phương"
        ) as standardized_location_name,
        "Địa phương" as original_location_name,
        CAST("2026 Dân số trung bình (Nghìn người)", 'Float64') as population_thousand,
        CAST("2026 Diện tích(Km2)(*)", 'Float64') as area_km2,
        CAST("2026 Mật độ dân số (Người/km2)", 'Float64') as density_per_km2
    FROM raw_pop
    WHERE original_location_name NOT IN ('Cả Nước', 'Đồng bằng sông Hồng', 'Trung du và miền núi phía Bắc', 'Bắc Trung Bộ và Duyên hải miền Trung', 'Tây Nguyên', 'Đông Nam Bộ', 'Đồng bằng sông Cửu Long')
)

SELECT
    standardized_location_name as location_name,
    original_location_name,
    population_thousand * 1000 as total_population,
    area_km2,
    density_per_km2
FROM cleaned
