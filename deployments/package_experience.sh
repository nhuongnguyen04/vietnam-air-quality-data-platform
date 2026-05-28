#!/bin/bash
# ===================================================================
# Script đóng gói Phiên Bản Trải Nghiệm Siêu Nhẹ (ClickHouse + Dashboard)
# Hệ thống: Vietnam Air Quality Data Platform
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

echo -e "${BLUE}=====================================================================${NC}"
echo -e "${GREEN}  ĐÓNG GÓI PHIÊN BẢN TRẢI NGHIỆM SIÊU NHẸ (LIGHTWEIGHT EXPERIENCE)   ${NC}"
echo -e "${BLUE}=====================================================================${NC}"

# 1. Xuất dữ liệu sạch ra tệp Parquet trước khi đóng gói
echo -e "${YELLOW}[1/6] Xuất dữ liệu sạch ra định dạng Parquet siêu nén...${NC}"
if [ -f "scripts/export_data.sh" ]; then
    ./scripts/export_data.sh
else
    echo -e "${RED}Lỗi: Không tìm thấy script 'scripts/export_data.sh'.${NC}"
    exit 1
fi

# 2. Flush dữ liệu và dừng các Docker container
echo -e "${YELLOW}[2/6] Dừng các Docker container đang chạy để dọn dẹp...${NC}"
docker compose down || true

# 3. Tạo thư mục tạm thời để đóng gói (staging)
STAGING_DIR="staging_experience"
echo -e "${YELLOW}[3/6] Tạo thư mục đóng gói tạm thời '${STAGING_DIR}'...${NC}"
rm -rf "$STAGING_DIR"
mkdir -p "$STAGING_DIR"

# 4. Sao chép và đổi tên các file cấu hình tối giản
echo -e "${YELLOW}[3/6] Chuẩn bị các cấu hình Docker và Môi trường...${NC}"
cp deployments/experience/docker-compose.yml "$STAGING_DIR/docker-compose.yml"

# Tạo file .env sạch, loại bỏ hoàn toàn Token API của tác giả
cat <<EOF > "$STAGING_DIR/.env"
# Vietnam Air Quality Data Platform — Cấu hình bản Trải nghiệm Siêu nhẹ
# Không cần API token, không sợ rò rỉ khóa bí mật.

CLICKHOUSE_DB=air_quality
CLICKHOUSE_USER=admin
CLICKHOUSE_PASSWORD=experience_only
GF_SECURITY_ADMIN_PASSWORD=admin
EOF

# 5. Sao chép dữ liệu và mã nguồn cần thiết, loại bỏ rác/cache
echo -e "${YELLOW}[4/6] Sao chép tệp hạt giống Parquet và mã nguồn Dashboard (Streamlit & Grafana)...${NC}"

# Sao chép các tệp dữ liệu hạt giống Parquet
cp -R clickhouse-seeds "$STAGING_DIR/"

# Sao chép Dashboard (Streamlit)
mkdir -p "$STAGING_DIR/python_jobs"
cp -R python_jobs/dashboard "$STAGING_DIR/python_jobs/"
# Dọn dẹp cache của Python trong thư mục đóng gói
find "$STAGING_DIR/python_jobs" -type d -name "__pycache__" -exec rm -rf {} + || true

# Sao chép cấu hình Giám sát (Grafana & Clickhouse Config)
mkdir -p "$STAGING_DIR/monitoring"
cp -R monitoring/grafana "$STAGING_DIR/monitoring/"
cp -R monitoring/clickhouse "$STAGING_DIR/monitoring/"

# Sao chép các script phụ trợ để tự động nạp dữ liệu khi khởi chạy
mkdir -p "$STAGING_DIR/scripts"
cp scripts/init-clickhouse.sql "$STAGING_DIR/scripts/" || true
cp scripts/import_data.sh "$STAGING_DIR/scripts/" || true

# 6. Tạo file HDSD nhanh (README) bên trong gói trải nghiệm
echo -e "${YELLOW}[5/6] Tạo tài liệu hướng dẫn sử dụng nhanh bên trong thư mục đóng gói...${NC}"
cat <<'EOF' > "$STAGING_DIR/README.md"
# Việt Nam Air Quality Data Platform — Bản Trải Nghiệm Siêu Nhẹ

Chào mừng bạn đến với phiên bản trải nghiệm siêu gọn của hệ thống phân tích chất lượng không khí Việt Nam!
Bản này chạy độc lập, offline, bảo mật và chỉ sử dụng các dữ liệu lịch sử đo lường được đóng gói sẵn.

## 🚀 Hướng dẫn khởi chạy nhanh trong 30 giây

### Yêu cầu hệ thống:
* Đã cài đặt **Docker** và **Docker Compose** trên máy (Windows qua WSL2, macOS, hoặc Linux).
* Bộ nhớ RAM trống tối thiểu: **4GB**.

### Các bước thực hiện:

1. **Khởi chạy hệ thống:**
   Mở terminal tại thư mục này và chạy lệnh:
   ```bash
   docker compose up -d
   ```

2. **Trải nghiệm các giao diện phân tích:**
   * **Streamlit Dashboard (Giao diện chính):** Truy cập địa chỉ `http://localhost:8501` trên trình duyệt.
   * **Grafana Dashboards:** Truy cập địa chỉ `http://localhost:3000` (đăng nhập bằng tài khoản `admin` / mật khẩu `admin`).

3. **Dừng hệ thống khi kết thúc:**
   Chạy lệnh:
   ```bash
   docker compose down
   ```

Chúc bạn có những trải nghiệm tuyệt vời với nền tảng phân tích dữ liệu chất lượng không khí Việt Nam!
EOF

# Chuẩn hóa line endings thành LF cho các script trong staging (tránh lỗi /bin/bash^M trên Windows)
echo -e "${YELLOW}Chuẩn hóa line endings (CRLF -> LF) để đảm bảo chạy mượt mà trên Windows/macOS/Linux...${NC}"
find "$STAGING_DIR" -type f -name "*.sh" -exec sed -i 's/\r$//' {} + || true
find "$STAGING_DIR" -type f -name "*.sql" -exec sed -i 's/\r$//' {} + || true

# 7. Nén toàn bộ thư mục staging thành file tar.gz và zip
OUTPUT_TAR="vietnam_aqi_experience.tar.gz"
OUTPUT_ZIP="vietnam_aqi_experience.zip"
echo -e "${YELLOW}[6/6] Đang nén toàn bộ thành hai định dạng '${OUTPUT_TAR}' và '${OUTPUT_ZIP}'...${NC}"

# Nén tar.gz
tar -czf "$OUTPUT_TAR" "$STAGING_DIR"

# Nén ZIP bằng Python (đảm bảo chạy chéo nền tảng, tạo cấu trúc thư mục đẹp)
python3 -c "
import zipfile, os
with zipfile.ZipFile('$OUTPUT_ZIP', 'w', zipfile.ZIP_DEFLATED) as zipf:
    for root, dirs, files in os.walk('$STAGING_DIR'):
        for file in files:
            filepath = os.path.join(root, file)
            arcname = os.path.join('vietnam_aqi_experience', os.path.relpath(filepath, '$STAGING_DIR'))
            zipf.write(filepath, arcname)
"

# 8. Dọn dẹp thư mục staging tạm thời
rm -rf "$STAGING_DIR"

echo -e "${BLUE}=====================================================================${NC}"
echo -e "${GREEN}🎉 ĐÓNG GÓI THÀNH CÔNG! Gói trải nghiệm của bạn đã sẵn sàng tại:     ${NC}"
echo -e "   1. Phiên bản Linux/macOS: ${YELLOW}${OUTPUT_TAR}${NC}"
echo -e "   2. Phiên bản Windows/macOS: ${YELLOW}${OUTPUT_ZIP}${NC}"
echo -e "${GREEN}Bạn chỉ cần gửi một trong hai file này để họ giải nén và trải nghiệm.${NC}"
echo -e "${BLUE}=====================================================================${NC}"
echo -e "Lưu ý: Để mở lại các container trên máy bạn sau khi đóng gói, chạy: docker compose up -d"
