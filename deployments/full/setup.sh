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

# 3. Thông báo điền API Key quan trọng
echo -e "${BLUE}=====================================================================${NC}"
echo -e "${YELLOW}👉 BƯỚC QUAN TRỌNG: Hãy điền các API Token của bạn vào file '.env':${NC}"
echo -e "   1. Mở file ${GREEN}.env${NC} bằng trình chỉnh sửa text."
echo -e "   2. Tìm và điền các khóa sau để kích hoạt cào dữ liệu thực tế:"
echo -e "      - ${GREEN}OPENWEATHER_API_TOKEN${NC} (Lấy miễn phí từ openweather.org)"
echo -e "      - ${GREEN}TOMTOM_API_KEY${NC} (Lấy miễn phí từ developer.tomtom.com)"
echo -e "      - ${GREEN}WAQI_TOKEN${NC} (Lấy miễn phí từ aqicn.org)"
echo -e "      - ${GREEN}GROQ_API_KEY${NC} (Cho tính năng Ask Data - Hỏi đáp AI)"
echo -e "      - ${GREEN}TELEGRAM_AQ_BOT_TOKEN${NC} / ${GREEN}CHAT_ID${NC} (Nếu muốn nhận cảnh báo)"
echo -e "${BLUE}=====================================================================${NC}"

read -p "Nhấn [Enter] sau khi bạn đã chỉnh sửa file .env (hoặc để tiếp tục chạy mặc định)..."

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
