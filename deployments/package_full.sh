#!/bin/bash
# ===================================================================
# Script đóng gói Phiên Bản Đầy Đủ (Full Stack Edition - <100MB)
# Hệ thống: Vietnam Air Quality Data Platform
# Phù hợp cho nộp hội đồng chấm đồ án (Bảo mật mã nguồn hoàn toàn)
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
echo -e "${GREEN}      ĐÓNG GÓI PHIÊN BẢN FULL STACK BẢO MẬT (<100MB SUBMISSION)    ${NC}"
echo -e "${BLUE}=====================================================================${NC}"

# 1. Tự động xuất dữ liệu lịch sử 14 ngày làm hạt giống dữ liệu (seeds)
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
STAGING_DIR="staging_full"
echo -e "${YELLOW}[3/6] Tạo thư mục đóng gói tạm thời '${STAGING_DIR}'...${NC}"
rm -rf "$STAGING_DIR"
mkdir -p "$STAGING_DIR"
mkdir -p "$STAGING_DIR/scripts"

# 4. Sao chép CHỈ các thành phần cấu hình và hạt giống (EXCLUDES all source code!)
echo -e "${YELLOW}[4/6] Sao chép cấu hình, tài liệu và hạt giống Parquet (Bảo mật mã nguồn)...${NC}"

# Sao chép docker-compose.full.yml đổi tên thành docker-compose.yml để người dùng chạy trực tiếp
cp deployments/full/docker-compose.yml "$STAGING_DIR/docker-compose.yml"

# Sao chép clickhouse-seeds
if [ -d "clickhouse-seeds" ]; then
    cp -R clickhouse-seeds "$STAGING_DIR/"
else
    echo -e "${RED}Lỗi: Không tìm thấy thư mục 'clickhouse-seeds'.${NC}"
    exit 1
fi

# Sao chép các tệp giám sát cấu hình (Grafana, ClickHouse config, Prometheus config)
if [ -d "monitoring" ]; then
    cp -R monitoring "$STAGING_DIR/"
fi

# Sao chép các file cấu hình cơ sở dữ liệu postgres khởi tạo
if [ -d "postgres" ]; then
    cp -R postgres "$STAGING_DIR/"
fi

# Sao chép chỉ các script được phép (Không chứa business logic hay scraping code nhạy cảm)
cp scripts/generate_secrets.py "$STAGING_DIR/scripts/"
cp scripts/init-clickhouse.sql "$STAGING_DIR/scripts/"
cp scripts/import_data.sh "$STAGING_DIR/scripts/"

# Sao chép hướng dẫn triển khai đồ án, cẩm nang vận hành và file setup
cp deployments/full/setup.sh "$STAGING_DIR/setup.sh"
cp DEPLOYMENT_GUIDE.md "$STAGING_DIR/"
cp HUONG_DAN_VAN_HANH.md "$STAGING_DIR/"
cp .env.example "$STAGING_DIR/"

# 5. Tạo file README.md chuyên nghiệp hướng dẫn hội đồng đồ án vận hành bản Full Stack
cat <<'EOF' > "$STAGING_DIR/README.md"
# Việt Nam Air Quality Data Platform — Phiên Bản Đầy Đủ (Full Stack Edition)

Chào mừng bạn đến với phiên bản đầy đủ và tự động hóa cao của hệ sinh thái phân tích chất lượng không khí Việt Nam!
Phiên bản này tích hợp trọn bộ các công cụ Data Engineering hiện đại từ Thu thập, Phân tích, Giám sát cho tới Quản trị Siêu dữ liệu (Metadata Management).

## 🚀 Các Dịch Vụ Được Tích Hợp
1. **Streamlit Dashboard (Giao diện chính):** http://localhost:8501
   * Tích hợp tính năng **Ask Data** hỗ trợ hỏi đáp dữ liệu bằng ngôn ngữ tự nhiên thông qua AI (Text-to-SQL).
2. **Apache Airflow 3 (Điều phối Ingestion & dbt):** http://localhost:8090 (admin / admin)
   * Tự động hóa hoàn toàn các luồng cào dữ liệu thời gian thực và biến đổi dữ liệu thông qua dbt.
3. **OpenMetadata (Quản trị dữ liệu & Catalog):** http://localhost:8585 (admin / admin)
   * Quản lý Lineage từ nguồn tới đích, kiểm thử chất lượng dữ liệu (Data Quality) và Data Dictionary.
4. **Grafana Dashboards (Giám sát hệ thống & Cảnh báo):** http://localhost:3000 (admin / admin)
   * Hệ thống cảnh báo tự động qua Telegram và đo lường tài nguyên.
5. **Prometheus (Đo lường cơ sở hạ tầng):** http://localhost:9090
   * Thu thập metrics hiệu năng từ máy chủ, container và database ClickHouse.

---

## 🛠️ Hướng dẫn Khởi chạy & Vận hành Đồ án

### 📋 Yêu cầu hệ thống:
* Đã cài đặt **Docker** và **Docker Compose** trên hệ điều hành (Windows qua WSL2, macOS, hoặc Linux).
* Bộ nhớ RAM tối thiểu: **8GB** (khuyến nghị **12GB - 16GB** để chạy mượt mà toàn bộ stack 11 containers).

### ⚡ Các bước thực hiện:

1. **Khởi tạo và Tạo khóa bảo mật tự động:**
   Mở terminal tại thư mục đã giải nén và chạy script:
   ```bash
   ./setup.sh
   ```
   *Script này sẽ sinh ngẫu nhiên toàn bộ mật khẩu, Fernet Key cho Airflow, mã hóa bảo mật cho hệ thống và tự động tạo file `.env`.*

2. **Cấu hình API Key (Tùy chọn - để cào dữ liệu thực tế):**
   Nếu bạn muốn kích hoạt luồng cào dữ liệu thực tế thời gian thực, mở file `.env` vừa được tạo và điền các API Key của bạn (OpenWeather, TomTom, WAQI, Groq AI).
   * 👉 **QUAN TRỌNG:** Xem hướng dẫn chi tiết cách đăng ký, lấy API Keys/Tokens từng bước và điền file `.env` tại tài liệu độc lập **HUONG_DAN_VAN_HANH.md** ở cùng thư mục giải nén.
   * *Nếu không điền, hệ thống vẫn hoạt động hoàn hảo với 14 ngày dữ liệu mẫu lịch sử đã được đóng gói sẵn trong ClickHouse!*

3. **Truy cập và Trải nghiệm:**
   Sau khi các service khởi chạy thành công, bạn có thể truy cập các địa chỉ sau:
   * **Dashboard Phân tích (Streamlit):** `http://localhost:8501` (Có sẵn dữ liệu lịch sử)
   * **Grafana Dashboards:** `http://localhost:3000` (admin/admin)
   * **Airflow 3:** `http://localhost:8090` (admin/admin)
   * **OpenMetadata Catalog:** `http://localhost:8585`

4. **Tắt hệ thống:**
   Chạy lệnh:
   ```bash
   docker compose down
   ```

---

*Lưu ý bảo mật: Toàn bộ mã nguồn cốt lõi (luồng cào Python, các model SQL dbt) đã được đóng gói và biên dịch sẵn bên trong các container Docker được kéo trực tiếp từ registry trung tâm. Điều này đảm bảo tính đóng gói độc lập của đồ án và chống rò rỉ mã nguồn khi chuyển giao hệ thống.*
EOF

# 6. Chuẩn hóa line endings thành LF để đảm bảo chạy mượt mà trên Windows (tránh lỗi /bin/bash^M)
echo -e "${YELLOW}Chuẩn hóa line endings (CRLF -> LF) cho tất cả các script...${NC}"
find "$STAGING_DIR" -type f -name "*.sh" -exec sed -i 's/\r$//' {} + || true
find "$STAGING_DIR" -type f -name "*.sql" -exec sed -i 's/\r$//' {} + || true

# 7. Nén toàn bộ thư mục staging thành tệp tar.gz và zip siêu nhẹ
OUTPUT_TAR="vietnam_aqi_full.tar.gz"
OUTPUT_ZIP="vietnam_aqi_full.zip"
echo -e "${YELLOW}[5/6] Đang nén toàn bộ thành hai định dạng '${OUTPUT_TAR}' và '${OUTPUT_ZIP}'...${NC}"

# Nén tar.gz
tar -czf "$OUTPUT_TAR" "$STAGING_DIR"

# Nén ZIP bằng Python (chạy chéo nền tảng tốt hơn, tạo cấu trúc thư mục cha đẹp)
python3 -c "
import zipfile, os
with zipfile.ZipFile('$OUTPUT_ZIP', 'w', zipfile.ZIP_DEFLATED) as zipf:
    for root, dirs, files in os.walk('$STAGING_DIR'):
        for file in files:
            filepath = os.path.join(root, file)
            arcname = os.path.join('vietnam_aqi_full', os.path.relpath(filepath, '$STAGING_DIR'))
            zipf.write(filepath, arcname)
"

# 8. Dọn dẹp thư mục staging tạm thời
echo -e "${YELLOW}[6/6] Dọn dẹp thư mục tạm staging...${NC}"
rm -rf "$STAGING_DIR"

echo -e "${BLUE}=====================================================================${NC}"
echo -e "${GREEN}🎉 ĐÓNG GÓI PHIÊN BẢN FULL STACK BẢO MẬT THÀNH CÔNG!                ${NC}"
echo -e "   1. Phiên bản Linux/macOS: ${YELLOW}${OUTPUT_TAR}${NC}"
echo -e "   2. Phiên bản Windows/macOS: ${YELLOW}${OUTPUT_ZIP}${NC}"
echo -e "${GREEN}Dung lượng tệp tin cực kỳ nhỏ nhẹ (<100MB), bảo mật mã nguồn tuyệt đối!${NC}"
echo -e "${GREEN}Sẵn sàng nộp hội đồng đồ án tốt nghiệp!                             ${NC}"
echo -e "${BLUE}=====================================================================${NC}"
echo -e "Lưu ý: Để khởi chạy lại hệ thống trên máy của bạn hiện tại, sử dụng: docker compose up -d"
