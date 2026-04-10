{% macro get_vietnam_region_3(province) %}
    -- Major 3-region mapping
    CASE
        WHEN LOWER({{ province }}) IN (
            'dien bien', 'hoa binh', 'lai chau', 'son la', 'lao cai', 'yen bai',
            'bac giang', 'bac kan', 'cao bang', 'ha giang', 'lang son', 'phu tho', 'quang ninh', 'thai nguyen', 'tuyen quang',
            'bac ninh', 'ha nam', 'ha noi', 'hai duong', 'hai phong', 'hung yen', 'nam dinh', 'ninh binh', 'thai binh', 'vinh phuc'
        ) THEN 'Northern'
        WHEN LOWER({{ province }}) IN (
            'ha tinh', 'nghe an', 'quang binh', 'quang tri', 'thanh hoa', 'thua thien hue',
            'binh dinh', 'binh thuan', 'da nang', 'khanh hoa', 'ninh thuan', 'phu yen', 'quang nam', 'quang ngai',
            'dak lak', 'dak nong', 'gia lai', 'kon tum', 'lam dong'
        ) THEN 'Central'
        WHEN LOWER({{ province }}) IN (
            'ba ria - vung tau', 'binh duong', 'binh phuoc', 'dong nai', 'ho chi minh', 'tay ninh',
            'an giang', 'bac lieu', 'ben tre', 'ca mau', 'can tho', 'dong thap', 'hau giang', 'kien giang', 'long an', 'soc trang', 'tien giang', 'tra vinh', 'vinh long'
        ) THEN 'Southern'
        ELSE 'Unknown'
    END
{% endmacro %}

{% macro get_vietnam_region_8(province) %}
    -- Standard 8-region mapping
    CASE
        WHEN LOWER({{ province }}) IN ('dien bien', 'hoa binh', 'lai chau', 'son la', 'lao cai', 'yen bai') 
            THEN 'Northwest'
        WHEN LOWER({{ province }}) IN ('bac giang', 'bac kan', 'cao bang', 'ha giang', 'lang son', 'phu tho', 'quang ninh', 'thai nguyen', 'tuyen quang') 
            THEN 'Northeast'
        WHEN LOWER({{ province }}) IN ('bac ninh', 'ha nam', 'ha noi', 'hai duong', 'hai phong', 'hung yen', 'nam dinh', 'ninh binh', 'thai binh', 'vinh phuc') 
            THEN 'Red River Delta'
        WHEN LOWER({{ province }}) IN ('ha tinh', 'nghe an', 'quang binh', 'quang tri', 'thanh hoa', 'thua thien hue') 
            THEN 'North Central'
        WHEN LOWER({{ province }}) IN ('binh dinh', 'binh thuan', 'da nang', 'khanh hoa', 'ninh thuan', 'phu yen', 'quang nam', 'quang ngai') 
            THEN 'South Central Coast'
        WHEN LOWER({{ province }}) IN ('dak lak', 'dak nong', 'gia lai', 'kon tum', 'lam dong') 
            THEN 'Central Highlands'
        WHEN LOWER({{ province }}) IN ('ba ria - vung tau', 'binh duong', 'binh phuoc', 'dong nai', 'ho chi minh', 'tay ninh') 
            THEN 'Southeast'
        WHEN LOWER({{ province }}) IN ('an giang', 'bac lieu', 'ben tre', 'ca mau', 'can tho', 'dong thap', 'hau giang', 'kien giang', 'long an', 'soc trang', 'tien giang', 'tra vinh', 'vinh long') 
            THEN 'Mekong Delta'
        ELSE 'Unknown'
    END
{% endmacro %}
