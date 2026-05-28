#!/bin/bash
# ===================================================================
# Script chạy tự động khi khởi tạo ClickHouse để import các tệp Parquet
# Vị trí mount: /docker-entrypoint-initdb.d/02-import.sh
# ===================================================================

set -e

echo "====================================================================="
echo "   TỰ ĐỘNG NẠP DỮ LIỆU LOGIC PARQUET KHỞI TẠO (AUTO IMPORT SEEDS)"
echo "====================================================================="

USER_FILES_DIR="/var/lib/clickhouse/user_files"

# Kiểm tra biến môi trường LOAD_SAMPLE_DATA để tùy chọn nạp dữ liệu mẫu
if [ "$LOAD_SAMPLE_DATA" = "false" ]; then
    echo "====================================================================="
    echo "[-] Bỏ qua tự động nạp dữ liệu mẫu (LOAD_SAMPLE_DATA=false)."
    echo "====================================================================="
    exit 0
fi

# Kiểm tra thư mục chứa tệp Parquet
if [ ! -d "$USER_FILES_DIR" ]; then
    echo "[-] Không tìm thấy thư mục user_files ($USER_FILES_DIR). Bỏ qua import."
    exit 0
fi

# Quét qua tất cả tệp .parquet trong thư mục
for FILEPATH in "$USER_FILES_DIR"/*.parquet; do
    # Kiểm tra nếu có bất kỳ file nào khớp
    [ -e "$FILEPATH" ] || continue
    
    FILENAME=$(basename "$FILEPATH")
    TABLE="${FILENAME%.parquet}"
    
    echo "[*] Đang xử lý tệp $FILENAME cho bảng air_quality.$TABLE..."
    
    # 1. Kiểm tra xem bảng đích đã có dữ liệu chưa để tránh ghi đè/trùng lặp
    ROWS=$(clickhouse-client --query "SELECT count() FROM air_quality.${TABLE}" 2>/dev/null || echo "0")
    
    if [ "$ROWS" -gt 0 ]; then
        echo "   [!] Bỏ qua: Bảng air_quality.${TABLE} đã chứa sẵn ${ROWS} dòng dữ liệu."
    else
        # 2. Thực hiện nạp dữ liệu từ file Parquet cực nhanh
        echo "   [+] Đang import dữ liệu từ $FILENAME..."
        clickhouse-client --query "INSERT INTO air_quality.${TABLE} SELECT * FROM file('${FILENAME}', 'Parquet')"
        
        # Xác minh kết quả
        NEW_ROWS=$(clickhouse-client --query "SELECT count() FROM air_quality.${TABLE}")
        echo "   [✓] Thành công! Đã nạp ${NEW_ROWS} dòng vào bảng air_quality.${TABLE}."
    fi
done

echo "====================================================================="
echo "   HOÀN THÀNH TỰ ĐỘNG NẠP DỮ LIỆU SEEDS!"
echo "====================================================================="
