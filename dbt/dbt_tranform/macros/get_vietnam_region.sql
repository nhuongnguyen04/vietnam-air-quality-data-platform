{% macro get_vietnam_region_3(province) %}
    -- Major 3-region mapping based on the 2026 34-province scope
    CASE
        WHEN {{ province }} IN (
            'Điện Biên', 'Lai Châu', 'Sơn La', 'Lào Cai',
            'Cao Bằng', 'Lạng Sơn', 'Tuyên Quang', 'Thái Nguyên', 'Phú Thọ', 'Quảng Ninh',
            'Hà Nội', 'Hải Phòng', 'Bắc Ninh', 'Hưng Yên', 'Ninh Bình'
        ) THEN 'Northern'
        WHEN {{ province }} IN (
            'Thanh Hóa', 'Nghệ An', 'Hà Tĩnh', 'Quảng Trị', 'Huế',
            'Đà Nẵng', 'Quảng Ngãi', 'Khánh Hòa',
            'Đắk Lắk', 'Gia Lai', 'Lâm Đồng'
        ) THEN 'Central'
        WHEN {{ province }} IN (
            'Hồ Chí Minh', 'Đồng Nai', 'Tây Ninh',
            'Cần Thơ', 'An Giang', 'Vĩnh Long', 'Cà Mau', 'Đồng Tháp'
        ) THEN 'Southern'
        ELSE 'Unknown'
    END
{% endmacro %}

{% macro get_vietnam_region_8(province) %}
    -- Standard 8-region mapping based on the 2026 34-province scope
    CASE
        WHEN {{ province }} IN ('Điện Biên', 'Lai Châu', 'Sơn La', 'Lào Cai') 
            THEN 'Northwest'
        WHEN {{ province }} IN ('Cao Bằng', 'Lạng Sơn', 'Tuyên Quang', 'Thái Nguyên', 'Phú Thọ', 'Quảng Ninh') 
            THEN 'Northeast'
        WHEN {{ province }} IN ('Hà Nội', 'Hải Phòng', 'Bắc Ninh', 'Hưng Yên', 'Ninh Bình') 
            THEN 'Red River Delta'
        WHEN {{ province }} IN ('Thanh Hóa', 'Nghệ An', 'Hà Tĩnh', 'Quảng Trị', 'Huế') 
            THEN 'North Central'
        WHEN {{ province }} IN ('Đà Nẵng', 'Quảng Ngãi', 'Khánh Hòa') 
            THEN 'South Central Coast'
        WHEN {{ province }} IN ('Đắk Lắk', 'Gia Lai', 'Lâm Đồng') 
            THEN 'Central Highlands'
        WHEN {{ province }} IN ('Hồ Chí Minh', 'Đồng Nai', 'Tây Ninh') 
            THEN 'Southeast'
        WHEN {{ province }} IN ('Cần Thơ', 'An Giang', 'Vĩnh Long', 'Cà Mau', 'Đồng Tháp') 
            THEN 'Mekong Delta'
        ELSE 'Unknown'
    END
{% endmacro %}
