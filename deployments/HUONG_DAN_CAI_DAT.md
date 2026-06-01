# HƯỚNG DẪN CÀI ĐẶT & KHỞI CHẠY HỆ THỐNG (INSTALLATION GUIDE)
## Nền Tảng Dữ Liệu Chất Lượng Không Khí Việt Nam (Vietnam Air Quality Data Platform)

Tài liệu này cung cấp hướng dẫn cài đặt chi tiết từng bước (step-by-step) để khởi chạy hệ thống phân tích chất lượng không khí Việt Nam trên máy tính cá nhân hoặc máy chủ mới với **2 phiên bản tùy chọn**:

1. **Phiên bản Đầy đủ (Full Stack Edition):** Thích hợp cho môi trường phát triển (Development) hoặc vận hành tự động (Production). Bao gồm đầy đủ 11 container: Ingestion (Scrapers), dbt, Airflow 3, ClickHouse, PostgreSQL, OpenMetadata, Elasticsearch, Grafana, Prometheus và Ask Data AI.
2. **Phiên bản Trải nghiệm Siêu nhẹ (Experience Edition):** Thích hợp để demo nhanh, chạy offline hoàn toàn. Chỉ gồm 3 container (ClickHouse + Streamlit + Grafana), sử dụng 14 ngày dữ liệu mẫu lịch sử Việt Nam đã được tích hợp sẵn dưới dạng Parquet siêu nén.

---

## 🛠️ YÊU CẦU CHUẨN BỊ TRƯỚC (PREREQUISITES)

Trước khi thực hiện cài đặt, máy chủ hoặc máy tính cá nhân của bạn cần đáp ứng các yêu cầu tối thiểu sau:
- **Công cụ bắt buộc:** Đã cài đặt **Docker** (phiên bản >= 24.0) và **Docker Compose** (phiên bản >= 2.20).
- **Trình duyệt web:** Google Chrome, Mozilla Firefox, hoặc Microsoft Edge (khuyên dùng Chrome).
- **Kết nối mạng:** Đường truyền internet ổn định để tải các Docker Images từ Registry trung tâm trong lần chạy đầu tiên.

---

## 💻 HƯỚNG DẪN CÀI ĐẶT CHI TIẾT THEO HỆ ĐIỀU HÀNH

Giải nén tệp `vietnam_aqi_platform.zip` vào một thư mục làm việc cụ thể và làm theo hướng dẫn dưới đây tương ứng với hệ điều hành của bạn:

### 🐧 1. Cài đặt trên hệ điều hành LINUX (Ubuntu / Debian / CentOS)
1. Mở cửa sổ Terminal và di chuyển đến thư mục đã giải nén:
   ```bash
   cd /path/to/extracted/vietnam_aqi_platform
   ```
2. Cấp quyền thực thi cho tập lệnh khởi tạo tự động:
   ```bash
   chmod +x setup.sh
   ```
3. Chạy tập lệnh cài đặt tương tác:
   ```bash
   ./setup.sh
   ```
4. **Tương tác dòng lệnh:**
   - Nhập `1` để cài đặt phiên bản **Đầy đủ (Full Stack)** hoặc `2` để cài đặt bản **Trải nghiệm (Experience)**.
   - Nhập `y` (hoặc nhấn [Enter]) khi được hỏi có muốn nạp dữ liệu mẫu lịch sử 14 ngày vào ClickHouse không.
5. Sau khi quá trình tải ảnh Docker và khởi động hoàn tất, hệ thống đã sẵn sàng hoạt động!

### 🍎 2. Cài đặt trên hệ điều hành macOS (Intel & Apple Silicon M1/M2/M3)
1. Mở ứng dụng **Terminal** có sẵn trên macOS.
2. Đảm bảo ứng dụng **Docker Desktop** đã được mở và đang chạy (biểu tượng cá voi màu xanh lá cây ở thanh menu trạng thái).
3. Di chuyển đến thư mục đã giải nén:
   ```bash
   cd /Users/ten_nguoi_dung/Downloads/vietnam_aqi_platform
   ```
4. Cấp quyền thực thi và khởi chạy tập lệnh:
   ```bash
   chmod +x setup.sh
   ./setup.sh
   ```
5. Thực hiện lựa chọn phiên bản (`1` hoặc `2`) và loại dữ liệu nạp theo hướng dẫn trực tiếp của script.

### 🪟 3. Cài đặt trên hệ điều hành WINDOWS (10 / 11)
Windows yêu cầu môi trường tương thích để chạy mượt mà toàn bộ container và thực thi được các lệnh shell `.sh`. Bạn hãy chọn một trong ba phương án dưới đây:

#### 👉 Cách 1: Sử dụng WSL2 (Khuyên dùng & Ổn định nhất)
Đây là phương pháp tối ưu nhất giúp hệ thống chạy với hiệu năng cao nhất trên Windows.
1. Mở terminal WSL2 của bạn (ví dụ ứng dụng **Ubuntu** đã cài trên Windows).
2. Đảm bảo Docker Desktop trên Windows đã tích hợp sẵn WSL2 (trong mục `Settings > Resources > WSL Integration > Kích hoạt Ubuntu`).
3. Di chuyển đến thư mục giải nén trên Windows thông qua phân vùng `/mnt/`:
   ```bash
   cd /mnt/c/Users/Ten_User/Downloads/vietnam_aqi_platform
   ```
4. Chạy script cài đặt:
   ```bash
   chmod +x setup.sh
   ./setup.sh
   ```

#### 👉 Cách 2: Sử dụng Git Bash (Chạy trực tiếp trên Windows)
Nếu máy bạn đã cài đặt Git cho Windows:
1. Click chuột phải tại thư mục giải nén `vietnam_aqi_platform` và chọn **Git Bash Here**.
2. Khởi chạy script cài đặt bằng lệnh:
   ```bash
   bash setup.sh
   ```
3. Làm theo các chỉ dẫn tương tác hiển thị trên màn hình Bash.

#### 👉 Cách 3: Sử dụng PowerShell (Khởi động thủ công không cần Script)
Nếu bạn không muốn sử dụng script bash tương tác, bạn có thể tự khởi chạy trực tiếp thông qua PowerShell:

*   **Lựa chọn A: Bản Trải nghiệm Siêu nhẹ (Experience Edition)**
    1. Mở PowerShell tại thư mục giải nén.
    2. Thực hiện sao chép file Docker Compose:
       ```powershell
       copy docker-compose.experience.yml docker-compose.yml
       ```
    3. Tạo thủ công file cấu hình môi trường `.env` bằng cách copy nội dung sau dán vào file `.env`:
       ```ini
       CLICKHOUSE_DB=air_quality
       CLICKHOUSE_USER=admin
       CLICKHOUSE_PASSWORD=experience_only
       LOAD_SAMPLE_DATA=true
       GF_SECURITY_ADMIN_PASSWORD=admin
       ```
    4. Khởi chạy Docker:
       ```powershell
       docker compose up -d
       ```

*   **Lựa chọn B: Bản Đầy đủ (Full Stack Edition)**
    1. Mở PowerShell tại thư mục giải nén.
    2. Sao chép file Docker Compose:
       ```powershell
       copy docker-compose.full.yml docker-compose.yml
       ```
    3. Chạy script Python tự động sinh file cấu hình và mật khẩu bảo mật:
       ```powershell
       python scripts/generate_secrets.py
       ```
    4. Khởi chạy Docker:
       ```powershell
       docker compose up -d
       ```

---

## 🚀 HƯỚNG DẪN SỬ DỤNG NHANH CÁC PHIÊN BẢN

Sau khi các container khởi động thành công, bạn hãy truy cập trình duyệt theo các địa chỉ dưới đây:

### 1. Phiên Bản Trải Nghiệm Siêu Nhẹ (Lightweight Experience)
- **Đặc điểm:** Yêu cầu RAM tối thiểu **4GB RAM**. Hệ thống chạy offline hoàn toàn, giao diện mượt mà và khởi động chỉ trong 15 giây.
- **Các cổng truy cập dịch vụ:**
  - **Streamlit Dashboard (Giao diện chính):** [http://localhost:8501](http://localhost:8501) (Có sẵn 14 ngày dữ liệu mẫu để phân tích).
  - **Grafana Dashboards:** [http://localhost:3000](http://localhost:3000) (Tài khoản: `admin` / Mật khẩu: `admin`) - Giám sát hiệu năng ClickHouse thô.

### 2. Phiên Bản Đầy Đủ (Full Stack Edition)
- **Đặc điểm:** Yêu cầu RAM trống khuyến nghị tối thiểu **8GB - 12GB RAM**. Tích hợp đầy đủ các dịch vụ Data Engineering doanh nghiệp.
- **Các cổng truy cập dịch vụ:**
  - **Streamlit Dashboard (AI Ask Data):** [http://localhost:8501](http://localhost:8501) - Tích hợp khung chatbot hỏi đáp Text-to-SQL tự nhiên bằng AI.
  - **Apache Airflow 3 (Điều phối):** [http://localhost:8090](http://localhost:8090) (Tài khoản: `admin` / Mật khẩu: `admin`) - Nơi quản lý các luồng cào dữ liệu tự động định kỳ.
  - **OpenMetadata (Quản trị dữ liệu):** [http://localhost:8585](http://localhost:8585) (Tài khoản: `admin` / Mật khẩu: `admin`) - Quản trị sơ đồ Lineage từ bảng thô tới bảng Marts.
  - **Grafana (Giám sát & Cảnh báo):** [http://localhost:3000](http://localhost:3000) (Tài khoản: `admin` / Mật khẩu: `admin`) - Xem biểu đồ hạ tầng và kích hoạt cảnh báo Telegram.

---

## ⚙️ CẨM NANG VẬN HÀNH & CẤU HÌNH API KEYS
- Đối với bản **Full Stack**, để hệ thống tự động cào dữ liệu thực tế và gửi cảnh báo về nhóm chat của bạn, vui lòng tham khảo chi tiết cẩm nang **`HUONG_DAN_VAN_HANH.md`** đi kèm trong cùng thư mục để lấy Token các API (OpenWeather, TomTom, WAQI, Groq AI, Telegram) và điền vào tệp `.env`.

---

## 🛑 DỪNG HỆ THỐNG
Để dừng hoạt động của toàn bộ các container và trả lại tài nguyên RAM cho máy tính của bạn, hãy chạy lệnh sau tại thư mục giải nén:
```bash
docker compose down
```

*Lưu ý bảo mật: Toàn bộ mã nguồn cốt lõi (luồng cào Python, các model SQL dbt) của bản Full Stack đã được đóng gói và biên dịch sẵn bên trong các container Docker được kéo trực tiếp từ registry trung tâm. Điều này đảm bảo tính đóng gói độc lập và tránh rò rỉ mã nguồn khi phân phối dự án.*
