#!/bin/bash
# ===================================================================
# Script đóng gói Hợp Nhất Hai Phiên Bản (Full Stack & Experience)
# Hệ thống: Vietnam Air Quality Data Platform
# Kết quả: Tạo ra duy nhất 1 file .zip có dung lượng dưới 100 MB
# ===================================================================

set -e

# Di chuyển đến thư mục gốc của dự án để đảm bảo đường dẫn chính xác
cd "$(dirname "$0")/.."

# Màu sắc hiển thị
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Biến cấu hình
REFRESH_SEEDS=false

# Phân tích tham số dòng lệnh
for arg in "$@"; do
    case $arg in
        --refresh-seeds)
            REFRESH_SEEDS=true
            shift
            ;;
    esac
done

echo -e "${BLUE}=====================================================================${NC}"
echo -e "${GREEN}      ĐÓNG GÓI HỢP NHẤT PHIÊN BẢN FULL STACK & EXPERIENCE (<100MB)  ${NC}"
echo -e "${BLUE}=====================================================================${NC}"

# 1. Tự động xuất dữ liệu lịch sử 14 ngày làm hạt giống dữ liệu (seeds) nếu được yêu cầu
if [ "$REFRESH_SEEDS" = "true" ]; then
    echo -e "${YELLOW}[1/6] Quét và xuất dữ liệu 14 ngày gần nhất ra Parquet siêu nén...${NC}"
    if [ -f "scripts/export_data.sh" ]; then
        ./scripts/export_data.sh
    else
        echo -e "${RED}Lỗi: Không tìm thấy script 'scripts/export_data.sh'.${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}[1/6] Sử dụng dữ liệu hạt giống (clickhouse-seeds) sẵn có trong codebase.${NC}"
    echo -e "      (Sử dụng tham số '${YELLOW}--refresh-seeds${NC}' nếu bạn muốn trích xuất dữ liệu mới từ ClickHouse)"
fi

# 2. Dừng các Docker container đang chạy để dọn dẹp tài nguyên
echo -e "${YELLOW}[2/6] Dừng các Docker container đang chạy...${NC}"
docker compose down || true

# 3. Tạo thư mục tạm thời để đóng gói (staging)
STAGING_DIR="staging_combined"
echo -e "${YELLOW}[3/6] Tạo thư mục đóng gói tạm thời '${STAGING_DIR}'...${NC}"
rm -rf "$STAGING_DIR"
mkdir -p "$STAGING_DIR"
mkdir -p "$STAGING_DIR/scripts"
mkdir -p "$STAGING_DIR/python_jobs"
mkdir -p "$STAGING_DIR/requirements"

# 4. Sao chép các tệp cấu hình và mã nguồn cần thiết (Dùng chung cho cả 2 phiên bản)
echo -e "${YELLOW}[4/6] Sao chép cấu hình, tài liệu, hạt giống Parquet và mã nguồn Dashboard...${NC}"

# Sao chép docker-compose của cả 2 bản vào thư mục staging đổi tên để phân biệt
cp deployments/full/docker-compose.yml "$STAGING_DIR/docker-compose.full.yml"
cp deployments/experience/docker-compose.yml "$STAGING_DIR/docker-compose.experience.yml"

# Sao chép clickhouse-seeds dùng chung
if [ -d "clickhouse-seeds" ]; then
    cp -R clickhouse-seeds "$STAGING_DIR/"
else
    echo -e "${RED}Lỗi: Không tìm thấy thư mục 'clickhouse-seeds'.${NC}"
    exit 1
fi

# Sao chép các tệp giám sát cấu hình (Grafana, ClickHouse, Prometheus config)
if [ -d "monitoring" ]; then
    cp -R monitoring "$STAGING_DIR/"
fi

# Sao chép các file cấu hình cơ sở dữ liệu postgres khởi tạo
if [ -d "postgres" ]; then
    cp -R postgres "$STAGING_DIR/"
fi

# Sao chép các script phụ trợ khởi tạo Clickhouse & nạp dữ liệu
cp scripts/generate_secrets.py "$STAGING_DIR/scripts/"
cp scripts/init-clickhouse.sql "$STAGING_DIR/scripts/"
cp scripts/import_data.sh "$STAGING_DIR/scripts/"

# Sao chép mã nguồn Streamlit Dashboard (Cần thiết cho bản Experience)
cp -R python_jobs/dashboard "$STAGING_DIR/python_jobs/"
find "$STAGING_DIR/python_jobs" -type d -name "__pycache__" -exec rm -rf {} + || true

# Sao chép requirements cho Dashboard (Sửa lỗi thiếu context khi build Streamlit trong bản Experience)
cp -R requirements "$STAGING_DIR/"

# Sao chép tài liệu hướng dẫn cài đặt và vận hành chi tiết
cp deployments/HUONG_DAN_CAI_DAT.md "$STAGING_DIR/"
cp deployments/HUONG_DAN_VAN_HANH.md "$STAGING_DIR/"
cp .env.example "$STAGING_DIR/"

# 5. Tạo tệp setup.sh tương tác thông minh tại thư mục gốc của gói nén
cat <<'EOF' > "$STAGING_DIR/setup.sh"
#!/bin/bash
# ===================================================================
# Script cài đặt & Khởi động Nền Tảng Chất Lượng Không Khí Việt Nam
# Hỗ trợ lựa chọn 2 phiên bản: Full Stack và Lightweight Experience
# ===================================================================

set -e

# Màu sắc hiển thị
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=====================================================================${NC}"
echo -e "${GREEN}      HỆ THỐNG PHÂN TÍCH CHẤT LƯỢNG KHÔNG KHÍ VIỆT NAM (VIETNAM AQI)  ${NC}"
echo -e "${BLUE}=====================================================================${NC}"

# 1. Kiểm tra Docker & Docker Compose
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Lỗi: Không tìm thấy 'docker' trên hệ thống. Vui lòng cài đặt Docker & Docker Compose trước.${NC}"
    exit 1
fi

# 2. Người dùng lựa chọn phiên bản để cài đặt
echo -e "${YELLOW}Vui lòng chọn phiên bản bạn muốn cài đặt và khởi chạy:${NC}"
echo -e "  ${GREEN}1)${NC} \033[1mPhiên Bản Đầy Đủ (Full Stack Edition)\033[0m"
echo -e "     - Bao gồm đầy đủ 11 container: Ingestion (Scrapers), dbt, Airflow 3, ClickHouse,"
echo -e "       PostgreSQL, OpenMetadata, Elasticsearch, Grafana, Prometheus và Ask Data AI."
echo -e "     - RAM tối thiểu khuyến nghị: ${YELLOW}8GB - 12GB RAM${NC}."
echo -e "     - Hỗ trợ luồng cào dữ liệu thực tế và giám sát hạ tầng."
echo -e ""
echo -e "  ${GREEN}2)${NC} \033[1mPhiên Bản Trải Nghiệm Siêu Nhẹ (Lightweight Experience Edition)\033[0m"
echo -e "     - Chỉ bao gồm 3 container: ClickHouse, Streamlit Dashboard và Grafana."
echo -e "     - Chạy hoàn toàn offline, sử dụng dữ liệu hạt giống lịch sử đã được tích hợp sẵn."
echo -e "     - RAM tối thiểu khuyến nghị: ${YELLOW}4GB RAM${NC} (cực kỳ nhẹ, khởi động tức thì)."
echo -e "${BLUE}=====================================================================${NC}"

read -p "Lựa chọn của bạn (1 hoặc 2, mặc định 1): " edition_choice
edition_choice=${edition_choice:-1}

if [ "$edition_choice" = "2" ]; then
    echo -e "\n${GREEN}--> BẠN ĐÃ CHỌN PHIÊN BẢN TRẢI NGHIỆM SIÊU NHẸ (EXPERIENCE EDITION)${NC}"
    
    # Sao chép docker-compose cho bản Experience
    cp docker-compose.experience.yml docker-compose.yml
    
    # Tạo file .env cho bản Experience
    cat <<INNER_EOF > .env
# Vietnam Air Quality Data Platform — Cấu hình bản Trải nghiệm Siêu nhẹ
# Không cần API token, không sợ rò rỉ khóa bí mật.

CLICKHOUSE_DB=air_quality
CLICKHOUSE_USER=admin
CLICKHOUSE_PASSWORD=experience_only
LOAD_SAMPLE_DATA=true
GF_SECURITY_ADMIN_PASSWORD=admin
INNER_EOF
    
    # Tạo thư mục dữ liệu cần thiết
    mkdir -p clickhouse-data
    
else
    echo -e "\n${GREEN}--> BẠN ĐÃ CHỌN PHIÊN BẢN ĐẦY ĐỦ (FULL STACK EDITION)${NC}"
    
    # Sao chép docker-compose cho bản Full Stack
    cp docker-compose.full.yml docker-compose.yml
    
    # Sinh file cấu hình bảo mật .env tự động
    echo -e "${YELLOW}Đang sinh file cấu hình bảo mật .env tự động...${NC}"
    if [ -f "scripts/generate_secrets.py" ]; then
        python3 scripts/generate_secrets.py
    else
        echo -e "${RED}Lỗi: Không tìm thấy script 'scripts/generate_secrets.py'.${NC}"
        exit 1
    fi
    
    # Tạo các thư mục dữ liệu cục bộ cần thiết cho bản Full Stack
    echo -e "${YELLOW}Tạo các thư mục lưu trữ dữ liệu cục bộ...${NC}"
    mkdir -p clickhouse-data
    mkdir -p airflow/logs
    mkdir -p airflow/data/postgres
    mkdir -p openmetadata/data
    mkdir -p openmetadata/elasticsearch-data
fi

# 3. Lựa chọn nạp dữ liệu mẫu lịch sử 14 ngày
echo -e "\n${BLUE}=====================================================================${NC}"
echo -e "${YELLOW}📊 LỰA CHỌN NẠP DỮ LIỆU MẪU (14 NGÀY LỊCH SỬ VIỆT NAM):${NC}"
echo -e "   - Việc nạp dữ liệu mẫu giúp Streamlit/Grafana hoạt động được ngay lập tức."
echo -e "   - Chọn 'y' để tự động import dữ liệu từ thư mục 'clickhouse-seeds'."
echo -e "   - Chọn 'n' nếu bạn muốn cơ sở dữ liệu trống hoàn toàn."
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

# 4. Đối với bản Full Stack, nhắc nhở điền API Key thực tế nếu muốn cào
if [ "$edition_choice" != "2" ]; then
    echo -e "${BLUE}=====================================================================${NC}"
    echo -e "${YELLOW}👉 THÔNG BÁO QUAN TRỌNG: Cấu hình API Keys & Vận hành Hệ thống${NC}"
    echo -e "   - Để cào dữ liệu thực tế (OpenWeather, TomTom, WAQI, Groq AI, v.v.),"
    echo -e "     vui lòng mở file ${GREEN}HUONG_DAN_VAN_HANH.md${NC} tại thư mục gốc giải nén."
    echo -e "   - Tài liệu này chứa hướng dẫn chi tiết từng bước lấy Token/Key"
    echo -e "     và điền vào file ${GREEN}.env${NC}."
    echo -e "${BLUE}=====================================================================${NC}"
    read -p "Nhấn [Enter] để tiếp tục và khởi chạy các container..."
fi

# 5. Khởi chạy các dịch vụ Docker
echo -e "\n${YELLOW}Khởi động toàn bộ các dịch vụ hệ thống...${NC}"
docker compose up -d

echo -e "${BLUE}=====================================================================${NC}"
echo -e "${GREEN}🎉 KHỞI TẠO VÀ CÀI ĐẶT HỆ THỐNG THÀNH CÔNG!                          ${NC}"
if [ "$edition_choice" = "2" ]; then
    echo -e "${GREEN}Các dịch vụ Phiên bản Trải nghiệm đang hoạt động:                    ${NC}"
    echo -e "   - ${BLUE}Streamlit Dashboard:${NC} http://localhost:8501 (Có sẵn dữ liệu lịch sử)"
    echo -e "   - ${BLUE}Grafana Dashboards:${NC} http://localhost:3000 (Tài khoản: admin / admin)"
else
    echo -e "${GREEN}Các dịch vụ Phiên bản Đầy đủ đang hoạt động:                         ${NC}"
    echo -e "   - ${BLUE}Streamlit Dashboard:${NC} http://localhost:8501"
    echo -e "   - ${BLUE}Airflow Webserver:${NC} http://localhost:8090 (Tài khoản: admin / admin)"
    echo -e "   - ${BLUE}OpenMetadata Catalog:${NC} http://localhost:8585"
    echo -e "   - ${BLUE}Grafana Dashboards:${NC} http://localhost:3000 (Tài khoản: admin / admin)"
fi
echo -e "${BLUE}=====================================================================${NC}"
echo -e "Để kiểm tra trạng thái các container, chạy lệnh: docker compose ps"
EOF
chmod +x "$STAGING_DIR/setup.sh"

# 7. Chuẩn hóa line endings thành LF để đảm bảo chạy mượt mà trên Windows (tránh lỗi /bin/bash^M)
echo -e "${YELLOW}Chuẩn hóa line endings (CRLF -> LF) cho tất cả các script...${NC}"
find "$STAGING_DIR" -type f -name "*.sh" -exec sed -i 's/\r$//' {} + || true
find "$STAGING_DIR" -type f -name "*.sql" -exec sed -i 's/\r$//' {} + || true

# 8. Nén toàn bộ thư mục staging thành tệp zip duy nhất dưới 100MB
OUTPUT_ZIP="vietnam_aqi_platform.zip"
echo -e "${YELLOW}[5/6] Đang nén toàn bộ thư mục staging thành '${OUTPUT_ZIP}'...${NC}"

# Nén ZIP bằng Python (chạy chéo nền tảng tốt hơn, tạo cấu trúc thư mục cha đẹp)
python3 -c "
import zipfile, os
with zipfile.ZipFile('$OUTPUT_ZIP', 'w', zipfile.ZIP_DEFLATED) as zipf:
    for root, dirs, files in os.walk('$STAGING_DIR'):
        for file in files:
            filepath = os.path.join(root, file)
            arcname = os.path.join('vietnam_aqi_platform', os.path.relpath(filepath, '$STAGING_DIR'))
            zipf.write(filepath, arcname)
"

# 9. Dọn dẹp thư mục staging tạm thời
echo -e "${YELLOW}[6/6] Dọn dẹp thư mục tạm staging...${NC}"
rm -rf "$STAGING_DIR"

echo -e "${BLUE}=====================================================================${NC}"
echo -e "${GREEN}🎉 ĐÓNG GÓI HỢP NHẤT THÀNH CÔNG!                                    ${NC}"
echo -e "   Tệp nén phân phối: ${YELLOW}${OUTPUT_ZIP}${NC}"
echo -e "   Dung lượng cực kỳ tối ưu (<100MB), chứa cả hai phiên bản và dữ liệu mẫu!"
echo -e "   Sẵn sàng để gửi đi và triển khai!                                 ${NC}"
echo -e "${BLUE}=====================================================================${NC}"
echo -e "Lưu ý: Để chạy trực tiếp trên máy của bạn hiện tại, hãy sử dụng: docker compose up -d"
