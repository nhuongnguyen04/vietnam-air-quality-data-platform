#!/bin/bash
# ===================================================================
# Script cài đặt & Khởi động Phiên Bản Đầy Đủ (Full Stack)
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
echo -e "${GREEN}      KHỞI TẠO VÀ CÀI ĐẶT PHIÊN BẢN ĐẦY ĐỦ (FULL STACK EDITION)      ${NC}"
echo -e "${BLUE}=====================================================================${NC}"

# 1. Kiểm tra Docker & Docker Compose
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Lỗi: Không tìm thấy 'docker' trên hệ thống. Vui lòng cài đặt Docker trước.${NC}"
    exit 1
fi

# 2. Sinh file .env với các khóa bảo mật ngẫu nhiên
echo -e "${YELLOW}[1/4] Đang sinh file cấu hình bảo mật .env tự động...${NC}"
if [ -f "scripts/generate_secrets.py" ]; then
    python3 scripts/generate_secrets.py
else
    echo -e "${RED}Lỗi: Không tìm thấy script 'scripts/generate_secrets.py'.${NC}"
    exit 1
fi

# 2.1. Lựa chọn nạp dữ liệu mẫu lịch sử 14 ngày
echo -e "\n${BLUE}=====================================================================${NC}"
echo -e "${YELLOW}📊 LỰA CHỌN NẠP DỮ LIỆU MẪU (14 NGÀY LỊCH SỬ VIỆT NAM):${NC}"
echo -e "   - Việc nạp dữ liệu mẫu giúp Streamlit/Grafana hoạt động được ngay lập tức offline."
echo -e "   - Chọn 'y' để tự động import dữ liệu Parquet từ thư mục 'clickhouse-seeds'."
echo -e "   - Chọn 'n' nếu bạn muốn ClickHouse sạch hoàn toàn để tự cào dữ liệu thực tế."
echo -e "${BLUE}=====================================================================${NC}"
read -p "Bạn có muốn tự động nạp dữ liệu mẫu 14 ngày vào ClickHouse không? (Y/n, mặc định Y): " load_seeds
load_seeds=${load_seeds:-Y}

if [[ "$load_seeds" =~ ^[Nn]$ ]]; then
    if grep -q "LOAD_SAMPLE_DATA=" .env; then
        sed -i 's/LOAD_SAMPLE_DATA=true/LOAD_SAMPLE_DATA=false/' .env
    else
        echo "LOAD_SAMPLE_DATA=false" >> .env
    fi
    echo -e "${YELLOW}--> Đã cấu hình: KHÔNG tự động nạp dữ liệu mẫu (LOAD_SAMPLE_DATA=false).${NC}"
else
    if grep -q "LOAD_SAMPLE_DATA=" .env; then
        sed -i 's/LOAD_SAMPLE_DATA=false/LOAD_SAMPLE_DATA=true/' .env
    else
        echo "LOAD_SAMPLE_DATA=true" >> .env
    fi
    echo -e "${GREEN}--> Đã cấu hình: TỰ ĐỘNG nạp dữ liệu mẫu (LOAD_SAMPLE_DATA=true).${NC}"
fi

# 3. Thông báo điền API Key quan trọng
echo -e "${BLUE}=====================================================================${NC}"
echo -e "${YELLOW}👉 THÔNG BÁO QUAN TRỌNG: Cấu hình API Keys & Vận hành Hệ thống${NC}"
echo -e "   - Để cào dữ liệu thực tế (OpenWeather, TomTom, WAQI, Groq AI, v.v.),"
echo -e "     vui lòng mở file ${GREEN}HUONG_DAN_VAN_HANH.md${NC} tại thư mục gốc."
echo -e "   - Tài liệu này chứa hướng dẫn chi tiết từng bước lấy Token/Key"
echo -e "     và điền vào file ${GREEN}.env${NC}."
echo -e "${BLUE}=====================================================================${NC}"

read -p "Nhấn [Enter] sau khi bạn đã xem/chỉnh sửa file .env (hoặc để tiếp tục chạy mặc định)..."

# 4. Tạo các thư mục dữ liệu cục bộ cần thiết
echo -e "${YELLOW}[2/4] Tạo các thư mục lưu trữ dữ liệu cục bộ...${NC}"
mkdir -p clickhouse-data
mkdir -p airflow/logs
mkdir -p airflow/data/postgres
mkdir -p openmetadata/data
mkdir -p openmetadata/elasticsearch-data

# 5. Khởi chạy các dịch vụ Docker (tải ảnh dựng sẵn từ Registry)
echo -e "${YELLOW}[3/4] Đang tải các Docker custom images từ Registry (quá trình này có thể mất vài phút)...${NC}"
docker compose pull

echo -e "${YELLOW}[4/4] Khởi động toàn bộ các dịch vụ Full Stack...${NC}"
docker compose up -d

echo -e "${BLUE}=====================================================================${NC}"
echo -e "${GREEN}🎉 KHỞI TẠO THÀNH CÔNG PHIÊN BẢN FULL STACK!                         ${NC}"
echo -e "${GREEN}Các dịch vụ đang hoạt động trong nền:                                ${NC}"
echo -e "   - ${BLUE}Streamlit Dashboard:${NC} http://localhost:8501"
echo -e "   - ${BLUE}Airflow Webserver:${NC} http://localhost:8090 (Tài khoản: admin / admin)"
echo -e "   - ${BLUE}OpenMetadata Catalog:${NC} http://localhost:8585"
echo -e "   - ${BLUE}Grafana Dashboards:${NC} http://localhost:3000 (Tài khoản: admin / admin)"
echo -e "${BLUE}=====================================================================${NC}"
echo -e "Để kiểm tra trạng thái các service, chạy lệnh: docker compose ps"
EOF
