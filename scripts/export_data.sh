#!/bin/bash
# ===================================================================
# Script xuất dữ liệu phân tích ra định dạng Parquet siêu nén
# Hệ thống: Vietnam Air Quality Data Platform
# ===================================================================

set -e

# Màu sắc hiển thị
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=====================================================================${NC}"
echo -e "${GREEN}      XUẤT DỮ LIỆU LOGIC RA FILE PARQUET SIÊU NÉN (EXPORT DATA)     ${NC}"
echo -e "${BLUE}=====================================================================${NC}"

# 1. Kiểm tra container ClickHouse có đang chạy không
CONTAINER_NAME="clickhouse"
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo -e "${YELLOW}ClickHouse container không chạy. Đang khởi chạy ClickHouse...${NC}"
    docker compose up -d clickhouse
    echo -e "${YELLOW}Đợi ClickHouse sẵn sàng...${NC}"
    sleep 5
fi

# 2. Tạo thư mục chứa dữ liệu xuất (clickhouse-seeds)
SEEDS_DIR="clickhouse-seeds"
echo -e "${YELLOW}[1/3] Tạo thư mục '${SEEDS_DIR}' trên host...${NC}"
rm -rf "$SEEDS_DIR"
mkdir -p "$SEEDS_DIR"

# 3. Danh sách các bảng cần xuất (chỉ xuất các bảng Streamlit Dashboard truy vấn)
TABLES=(
  # Bảng Fact (Đo lường & Tổng hợp)
  "fct_air_quality_ward_level_hourly"
  "fct_air_quality_summary_daily"
  "fct_air_quality_summary_hourly"
  "fct_ow__ward_hourly"
  "fct_traffic_ward_hourly"
  "stg_tomtom__flow"
  "stg_aqiin__measurements"
  
  # Bảng Dimension & Lookup
  "dim_administrative_units"
  "unified_stations_metadata"
  "vn_station_coordinates"
  "vn_traffic_profile"
  "vietnam_wards_with_osm"
  "vietnam_wards_2026"
  "pollutants"
  "stg_core__stations"
  
  # Bảng Data Marts (Phân tích)
  "dm_platform_source_health"
  "dm_traffic_hourly_trend"
  "dm_traffic_pollution_correlation_daily"
  "dm_pollutant_source_fingerprint"
  "dm_aqi_compliance_standards"
)

# 4. Xuất dữ liệu qua pipe stdout của docker exec
echo -e "${YELLOW}[2/3] Bắt đầu xuất từng bảng thành tệp Parquet (Lọc 14 ngày gần nhất)...${NC}"
for TABLE in "${TABLES[@]}"; do
    echo -e "   -> Đang xuất bảng ${GREEN}air_quality.${TABLE}${NC}..."
    
    # Xác định câu lệnh SELECT phù hợp để lọc dữ liệu 14 ngày cho các bảng lớn (giúp tệp nén < 100MB)
    QUERY="SELECT * FROM air_quality.${TABLE}"
    if [ "$TABLE" = "fct_air_quality_ward_level_hourly" ] || \
       [ "$TABLE" = "fct_air_quality_summary_hourly" ] || \
       [ "$TABLE" = "fct_ow__ward_hourly" ] || \
       [ "$TABLE" = "fct_traffic_ward_hourly" ] || \
       [ "$TABLE" = "dm_traffic_hourly_trend" ]; then
        QUERY="SELECT * FROM air_quality.${TABLE} WHERE datetime_hour >= now() - INTERVAL 14 DAY"
    elif [ "$TABLE" = "fct_air_quality_summary_daily" ] || \
         [ "$TABLE" = "dm_traffic_pollution_correlation_daily" ] || \
         [ "$TABLE" = "dm_pollutant_source_fingerprint" ]; then
        QUERY="SELECT * FROM air_quality.${TABLE} WHERE date >= today() - 14"
    elif [ "$TABLE" = "stg_tomtom__flow" ] || \
         [ "$TABLE" = "stg_aqiin__measurements" ]; then
        QUERY="SELECT * FROM air_quality.${TABLE} WHERE timestamp_utc >= now() - INTERVAL 14 DAY"
    fi

    # Kiểm tra xem bảng có tồn tại và có dữ liệu không
    EXISTS=$(docker exec -i "$CONTAINER_NAME" clickhouse-client --query "SELECT count() FROM (${QUERY})" 2>/dev/null || echo "0")
    
    if [ "$EXISTS" -gt 0 ]; then
        # Xuất trực tiếp sang định dạng Parquet thông qua pipe stdout (sở hữu hoàn toàn bởi host user)
        docker exec -i "$CONTAINER_NAME" clickhouse-client --query "${QUERY} FORMAT Parquet" > "${SEEDS_DIR}/${TABLE}.parquet"
        FILE_SIZE=$(ls -lh "${SEEDS_DIR}/${TABLE}.parquet" | awk '{print $5}')
        echo -e "      ${GREEN}✓ Thành công!${NC} Kích thước: ${YELLOW}${FILE_SIZE}${NC} (${EXISTS} dòng)"
    else
        echo -e "      ${RED}⚠ Bỏ qua:${NC} Bảng trống hoặc không có dữ liệu trong 14 ngày qua."
    fi
done

# 5. Hoàn thành
TOTAL_SIZE=$(du -sh "$SEEDS_DIR" | awk '{print $1}')
echo -e "${BLUE}=====================================================================${NC}"
echo -e "${GREEN}🎉 HOÀN THÀNH XUẤT DỮ LIỆU!                                          ${NC}"
echo -e "Tổng dung lượng thư mục ${SEEDS_DIR}: ${YELLOW}${TOTAL_SIZE}${NC}"
echo -e "${BLUE}=====================================================================${NC}"
