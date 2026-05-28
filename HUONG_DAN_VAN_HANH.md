# CẨM NANG VẬN HÀNH & HƯỚNG DẪN CẤU HÌNH TOKEN
## Hệ thống Phân tích Chất lượng Không khí Việt Nam (Vietnam Air Quality Data Platform)

Tài liệu này là cẩm nang hướng dẫn chi tiết dành cho quản trị viên (System Administrator) và kỹ sư dữ liệu (Data Engineer) để vận hành, quản trị hệ thống, đăng ký các API Keys, điền tệp cấu hình `.env` và giám sát nền tảng dữ liệu chất lượng không khí.

---

## 📋 MỤC LỤC
1. [TỔNG QUAN CÁC DỊCH VỤ & CỔNG TRUY CẬP](#1-tổng-quan-các-dịch-vụ--cổng-truy-cập)
2. [HƯỚNG DẪN CHI TIẾT ĐĂNG KÝ LẤY API KEYS & TOKENS](#2-hướng-dẫn-chi-tiết-đăng-ký-lấy-api-keys--tokens)
   * [2.1. OpenWeather API Token (Dữ liệu Thời tiết & Ô nhiễm)](#21-openweather-api-token-dữ-liệu-thời-tiết--ô-nhiễm)
   * [2.2. TomTom API Key (Dữ liệu Giao thông)](#22-tomtom-api-key-dữ-liệu-giao-thông)
   * [2.3. WAQI (AQICN) Token (Trạm quan trắc toàn cầu)](#23-waqi-aqicn-token-trạm-quan-trắc-toàn-cầu)
   * [2.4. Groq API Key (AI Hỏi đáp tự nhiên Ask Data)](#24-groq-api-key-ai-hỏi-đáp-tự nhiên-ask-data)
   * [2.5. Telegram Bot & Chat ID (Hệ thống Cảnh báo tự động)](#25-telegram-bot--chat-id-hệ-thống-cảnh-báo-tự-động)
   * [2.6. Google Drive OAuth Credentials (Đồng bộ Lưu trữ đám mây)](#26-google-drive-oauth-credentials-đồng-bộ-lưu-trữ-đám-mây)
   * [2.7. Mapbox Access Token (Bản đồ trực quan hóa)](#27-mapbox-access-token-bản-đồ-trực-quan-hóa)
3. [ÁNH XẠ CẤU HÌNH FILE `.env`](#3-ánh-xạ-cấu-hình-file-env)
4. [HƯỚNG DẪN KHỞI CHẠY & VẬN HÀNH (OPERATIONS RUNBOOK)](#4-hướng-dẫn-khởi-chạy--vận-hành-operations-runbook)
   * [4.1. Khởi động và Dừng hệ thống bằng Docker Compose](#41-khởi-động-và-dừng-hệ-thống-bằng-docker-compose)
   * [4.2. Vận hành luồng xử lý dữ liệu tự động (Apache Airflow)](#42-vận-hành-luồng-xử-lý-dữ-liệu-tự-động-apache-airflow)
   * [4.3. Xử lý sự cố thường gặp (Troubleshooting)](#43-xử-ly-sự-cố-thường-gặp-troubleshooting)
5. [HƯỚNG DẪN QUẢN TRỊ HỆ THỐNG (SYSTEM ADMINISTRATION)](#5-hướng-dẫn-quản-trị-hệ-thống-system-administration)
   * [5.1. Quản trị Cơ sở dữ liệu ClickHouse](#51-quản-trị-cơ-sở-dữ-liệu-clickhouse)
   * [5.2. Quản lý Siêu dữ liệu & Lineage với OpenMetadata](#52-quản-lý-siêu-dữ-liệu--lineage-với-openmetadata)
   * [5.3. Quản trị Dashboard và Cảnh báo với Grafana](#53-quản-trị-dashboard-và-cảnh-báo-với-grafana)

---

## 1. TỔNG QUAN CÁC DỊCH VỤ & CỔNG TRUY CẬP

Khi chạy phiên bản Đầy đủ (Full Stack Edition), toàn bộ hệ sinh thái dịch vụ sẽ chạy độc lập trong các Docker containers. Dưới đây là bảng tra cứu cổng dịch vụ:

| Dịch vụ | Địa chỉ truy cập | Tài khoản / Mật khẩu mặc định | Vai trò chính |
| :--- | :--- | :--- | :--- |
| **Streamlit Dashboard** | `http://localhost:8501` | *Không yêu cầu đăng nhập* | Giao diện người dùng, bản đồ tương tác, biểu đồ phân tích và tính năng **Ask Data** hỏi đáp AI. |
| **Apache Airflow** | `http://localhost:8090` | `admin` / `admin` (hoặc mật khẩu ngẫu nhiên trong `.env`) | Điều phối (Orchestration) toàn bộ luồng cào dữ liệu định kỳ, biến đổi dbt và đồng bộ. |
| **OpenMetadata** | `http://localhost:8585` | `admin` / `admin` | Cổng Quản trị Siêu dữ liệu (Metadata Catalog), Glossary, Data Quality và sơ đồ Lineage từ nguồn tới đích. |
| **Grafana** | `http://localhost:3000` | `admin` / `admin` | Dashboard giám sát hạ tầng máy chủ, hiệu năng cơ sở dữ liệu ClickHouse và kích hoạt cảnh báo Telegram. |
| **Prometheus** | `http://localhost:9090` | *Không yêu cầu đăng nhập* | Thu thập và lưu trữ metrics tài nguyên hệ thống. |
| **ClickHouse HTTP** | `http://localhost:8123` | `admin` / *(mật khẩu trong .env)* | Cổng giao tiếp HTTP API của ClickHouse Database. |

---

## 2. HƯỚNG DẪN CHI TIẾT ĐĂNG KÝ LẤY API KEYS & TOKENS

Hệ thống được thiết kế để tự động hóa hoàn toàn. Tuy nhiên, để cào dữ liệu thực tế tại Việt Nam, bạn cần đăng ký các tài khoản dịch vụ dưới đây để nhận Token.

### 2.1. OpenWeather API Token (Dữ liệu Thời tiết & Ô nhiễm)
Dùng để thu thập dữ liệu thời tiết thực tế (nhiệt độ, độ ẩm, sức gió) và nồng độ chất ô nhiễm không khí (PM2.5, PM10, NO2, CO, SO3, O3) cho 62 tỉnh thành Việt Nam.

1. Truy cập trang web: [https://openweathermap.org/api](https://openweathermap.org/api) và click **Sign Up** để tạo tài khoản mới.
2. Sau khi xác minh email, đăng nhập và chọn mục **API Keys** trên thanh menu tài khoản của bạn (hoặc truy cập trực tiếp [https://home.openweathermap.org/api_keys](https://home.openweathermap.org/api_keys)).
3. Tại phần **Create Key**, đặt tên (ví dụ: `vietnam_aqi_platform`) và nhấn **Generate**.
4. Sao chép chuỗi mã hex dài 32 ký tự. Đây là mã `OPENWEATHER_API_TOKEN` của bạn.
5. *Lưu ý về Gói cước (Free Tier):* Gói mặc định miễn phí cho phép cào **1,000 requests/ngày** và giới hạn tần suất 60 requests/phút. Tần suất cào 15 phút một lần của hệ thống hoàn toàn nằm trong hạn mức miễn phí này.

### 2.2. TomTom API Key (Dữ liệu Giao thông)
Dùng để cào dữ liệu mật độ giao thông, độ trễ và tốc độ di chuyển thực tế tại các tuyến đường trọng điểm xung quanh các trạm đo AQI ở Hà Nội và TP.HCM.

1. Truy cập cổng thông tin lập trình viên: [https://developer.tomtom.com/](https://developer.tomtom.com/) và click **Register** để tạo tài khoản.
2. Đăng nhập và truy cập **Dashboard** cá nhân.
3. Click vào **Keys** ở menu bên trái, sau đó click **Add a Key** ở góc trên bên phải.
4. Đặt tên App (ví dụ: `AQI_Traffic_Ingest`) và chọn các dịch vụ cần kích hoạt (Khuyên nghị: Tích chọn tất cả hoặc tối thiểu chọn **Search API** và **Routing API**). Nhấn **Create Key**.
5. Sao chép chuỗi ký tự dài tại cột **Consumer API Key**. Đây chính là `TOMTOM_API_KEY` của bạn.
6. *Hạn mức miễn phí:* **2,500 requests/ngày** miễn phí, cực kỳ dư dả cho hệ thống cào mỗi giờ.

### 2.3. WAQI (AQICN) Token (Trạm quan trắc toàn cầu)
Dùng để thu thập dữ liệu chỉ số AQI trực tiếp từ ~540 trạm đo thực địa của mạng lưới AQICN tại Việt Nam.

1. Truy cập trang cấp token: [https://aqicn.org/data-platform/token/](https://aqicn.org/data-platform/token/)
2. Điền đầy đủ thông tin vào form đăng ký:
   * **Name**: Tên của bạn.
   * **Email**: Địa chỉ email để nhận token.
   * **Purpose**: Điền mục đích nghiên cứu học tập (ví dụ: `University Graduation Thesis`).
3. Click **Submit**. Hệ thống sẽ gửi một liên kết xác nhận và Token trực tiếp về email của bạn sau vài phút.
4. Sao chép chuỗi token nhận được. Điền giá trị này vào `AQICN_API_TOKEN` (hoặc `WAQI_TOKEN` tùy phiên bản cấu hình).

### 2.4. Groq API Key (AI Hỏi đáp tự nhiên Ask Data)
Dùng làm bộ não LLM để chuyển đổi câu hỏi bằng ngôn ngữ tự nhiên của người dùng (Text-to-SQL) thành mã Clickhouse SQL chạy trực tiếp trên Dashboard Streamlit mà không làm rò rỉ cơ sở dữ liệu.

1. Truy cập trang quản trị Groq Cloud: [https://console.groq.com/](https://console.groq.com/) và đăng nhập bằng tài khoản Google hoặc email.
2. Tại menu bên trái, chọn mục **API Keys**.
3. Click nút **Create API Key**. Đặt tên cho khóa (ví dụ: `vietnam_aqi_askdata`) và nhấn **Generate**.
4. **BẮT BUỘC:** Sao chép mã API Key ngay lập tức vì Groq sẽ ẩn mã này sau khi đóng bảng thông báo. Mã này có tiền tố `gsk_...`.
5. Đây là giá trị điền vào `GROQ_API_KEY` của bạn.
6. Hệ thống sử dụng mô hình mã nguồn mở hiệu năng cao `qwen/qwen3-32b` (hoặc các dòng Llama-3 tương đương) giúp dịch Text-to-SQL cực nhanh và hoàn toàn miễn phí trong hạn mức cơ bản của Groq.

### 2.5. Telegram Bot & Chat ID (Hệ thống Cảnh báo tự động)
Hệ thống sử dụng Telegram để gửi cảnh báo tự động khi phát hiện ô nhiễm không khí vượt ngưỡng hoặc khi có container dịch vụ gặp sự cố kỹ thuật.

#### Bước A: Tạo Bot Telegram mới
1. Mở ứng dụng Telegram và tìm kiếm tài khoản chính thức **@BotFather** (có tích xanh).
2. Gửi tin nhắn lệnh `/newbot` để bắt đầu tạo bot.
3. Nhập **Tên hiển thị** cho Bot (ví dụ: `Vietnam Air Quality Alerter`).
4. Nhập **Username** cho Bot. Username bắt buộc phải kết thúc bằng chữ `bot` (ví dụ: `vnaqi_alert_bot`).
5. Sau khi tạo thành công, @BotFather sẽ gửi cho bạn một chuỗi **HTTP API Token** (Ví dụ: `7182938472:AAHjdf83...`). Đây là `TELEGRAM_AQ_BOT_TOKEN` và `TELEGRAM_SYS_BOT_TOKEN` của bạn.

#### Bước B: Lấy Chat ID của nhóm hoặc tài khoản cá nhân
Hệ thống hỗ trợ gửi cảnh báo đến Nhóm chat (cho cảnh báo ô nhiễm) hoặc Chat cá nhân (cho cảnh báo lỗi hệ thống gửi đến Admin).

* **Cách 1: Gửi đến tài khoản cá nhân (Admin):**
  1. Tìm kiếm bot **@userinfobot** trên Telegram và nhấn **Start**.
  2. Bot sẽ trả về ngay lập tức chuỗi số ID của bạn (Ví dụ: `182746284`). Đây chính là Chat ID của bạn.
  3. Điền giá trị này vào `TELEGRAM_SYS_CHAT_ID`.
  
* **Cách 2: Gửi đến Nhóm chat (Group):**
  1. Tạo một nhóm Telegram mới và **thêm Bot vừa tạo** ở Bước A vào nhóm đó.
  2. Thêm bot **@RawDataBot** (hoặc bất kỳ bot kiểm tra ID nào) vào nhóm tạm thời.
  3. Giao diện bot sẽ gửi một đoạn tin nhắn dạng JSON chứa thông tin nhóm. Tìm khóa `"chat"` -> `"id"` (ID nhóm chat Telegram luôn có tiền tố dấu trừ, ví dụ: `-100192847291`).
  4. Sau khi lấy được ID, bạn có thể xóa `@RawDataBot` ra khỏi nhóm.
  5. Điền giá trị này vào `TELEGRAM_AQ_CHAT_ID`.

### 2.6. Google Drive OAuth Credentials (Đồng bộ Lưu trữ đám mây)
Dùng để tải các tệp nén sao lưu dữ liệu ClickHouse hàng ngày lên Google Drive của bạn nhằm phòng tránh mất dữ liệu máy chủ vật lý.

1. Truy cập Google Cloud Console: [https://console.cloud.google.com/](https://console.cloud.google.com/)
2. Tạo một dự án mới (Project) đặt tên là `AQI-Data-Platform`.
3. Bật thư viện API: Chọn **Enabled APIs & Services** -> click **Enable APIs and Services** -> Tìm kiếm và bật **Google Drive API**.
4. Cấu hình màn hình đồng ý OAuth (OAuth Consent Screen):
   * Chọn User Type là **External**.
   * Điền thông tin App name, email liên hệ và nhấn Save.
   * Tại tab **Test Users**, thêm email Google cá nhân của bạn để cho phép truy cập thử nghiệm.
5. Tạo thông tin xác thực (Credentials):
   * Click **Create Credentials** -> Chọn **OAuth client ID**.
   * Application Type chọn **Web application** hoặc **Desktop app** (Khuyên dùng: Desktop App để lấy refresh token thủ công dễ dàng nhất).
   * Đặt tên và nhấn Create.
   * Tải tệp JSON chứa cấu hình về máy. Bạn sẽ lấy được `GDRIVE_CLIENT_ID` và `GDRIVE_CLIENT_SECRET`.
6. Lấy `GDRIVE_REFRESH_TOKEN`:
   * Chạy script Python hỗ trợ trong dự án để xác thực trình duyệt và nhận Refresh Token vĩnh viễn:
     ```bash
     python3 scripts/get_gdrive_refresh_token.py
     ```
   * Trình duyệt sẽ mở ra yêu cầu bạn cấp quyền truy cập Google Drive. Sau khi đồng ý, terminal sẽ in ra chuỗi Refresh Token.

### 2.7. Mapbox Access Token (Bản đồ trực quan hóa)
Dùng để hiển thị các lớp bản đồ tùy biến, ảnh vệ tinh, địa hình 3D mượt mà trên dashboard.

1. Truy cập trang: [https://account.mapbox.com/](https://account.mapbox.com/) và đăng ký tài khoản miễn phí.
2. Sau khi xác minh, truy cập Dashboard và sao chép mã **Default public token** hiển thị ở trang chủ (bắt đầu bằng `pk.eyJ...`).
3. Điền giá trị này vào `MAPBOX_ACCESS_TOKEN` trong `.env`.

---

## 3. ÁNH XẠ CẤU HÌNH FILE `.env`

Tệp `.env` nằm tại thư mục gốc là tệp duy nhất điều khiển toàn bộ cấu hình bảo mật, kết nối cơ sở dữ liệu và API Keys của hệ thống. Dưới đây là bảng ánh xạ chi tiết để bạn điền chính xác:

```ini
# ===================================================================
# THÔNG TIN KẾT NỐI DATABASE CLICKHOUSE
# ===================================================================
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=admin
# Điền mật khẩu mạnh của bạn (Không sử dụng mật khẩu mặc định khi lên Production)
CLICKHOUSE_PASSWORD=your_strong_password_here
CLICKHOUSE_DB=air_quality
# Tự động nạp 14 ngày dữ liệu mẫu khi khởi chạy lần đầu (y/n)
LOAD_SAMPLE_DATA=true

# ===================================================================
# API TOKENS DÙNG CHO TIẾN TRÌNH CÀO DỮ LIỆU TỰ ĐỘNG
# ===================================================================
# 1. OpenWeather API Key (Lấy từ mục 2.1)
OPENWEATHER_API_TOKEN=your_openweather_api_token_here

# 2. TomTom API Key (Lấy từ mục 2.2)
TOMTOM_API_KEY=your_tomtom_api_key_here

# 3. WAQI (AQICN) Token (Lấy từ mục 2.3)
AQICN_API_TOKEN=your_aqicn_api_token_here

# ===================================================================
# CẤU HÌNH CHATBOT HỎI ĐÁP AI (ASK DATA TEXT-TO-SQL)
# ===================================================================
# Khóa API của Groq (Lấy từ mục 2.4 - bắt đầu bằng gsk_...)
GROQ_API_KEY=your_groq_api_key_here
GROQ_MODEL=qwen/qwen3-32b
TEXT_TO_SQL_URL=http://text-to-sql:8000
TEXT_TO_SQL_TIMEOUT_SECONDS=90

# Tài khoản truy cập Clickhouse Read-Only dành riêng cho AI (Tăng tính bảo mật)
TEXT_TO_SQL_CLICKHOUSE_USER=aqi_reader
TEXT_TO_SQL_CLICKHOUSE_PASSWORD=aqi_reader_pass_123
TEXT_TO_SQL_PREVIEW_SECRET=long_random_preview_signing_secret_here

# ===================================================================
# CẤU HÌNH HỆ THỐNG ĐIỀU PHỐI ORCHESTRATION (APACHE AIRFLOW)
# ===================================================================
AIRFLOW_ADMIN_USERNAME=airflow_admin
AIRFLOW_ADMIN_PASSWORD=your_strong_airflow_pass_here
# Khóa mã hóa Fernet (Tự động sinh khi chạy ./setup.sh)
AIRFLOW__CORE__FERNET_KEY=generated_fernet_key_here
AIRFLOW_API_SECRET_KEY=generated_api_secret_key_here
AIRFLOW_API_AUTH_JWT_SECRET=generated_jwt_secret_here
AIRFLOW_WEBSERVER_SECRET_KEY=generated_webserver_secret_here

# ===================================================================
# CẤU HÌNH HỆ THỐNG CẢNH BÁO TELEGRAM (Lấy từ mục 2.5)
# ===================================================================
# Gửi cảnh báo ô nhiễm không khí vượt ngưỡng cho người dân
TELEGRAM_AQ_BOT_TOKEN=your_telegram_bot_token_here
TELEGRAM_AQ_CHAT_ID=-100192847291

# Gửi cảnh báo lỗi hệ thống, downtime dịch vụ cho Admin
TELEGRAM_SYS_BOT_TOKEN=your_telegram_bot_token_here
TELEGRAM_SYS_CHAT_ID=182746284

# ===================================================================
# CẤU HÌNH BACKUP TỰ ĐỘNG LÊN GOOGLE DRIVE (Lấy từ mục 2.6)
# ===================================================================
GDRIVE_CLIENT_ID=your_gdrive_client_id_here
GDRIVE_CLIENT_SECRET=your_gdrive_client_secret_here
GDRIVE_REFRESH_TOKEN=your_gdrive_refresh_token_here
GDRIVE_ROOT_FOLDER_ID=your_gdrive_folder_id_here

# ===================================================================
# CẤU HÌNH BẢN ĐỒ MAPBOX (Lấy từ mục 2.7)
# ===================================================================
MAPBOX_ACCESS_TOKEN=your_mapbox_public_token_here
```

---

## 4. HƯỚNG DẪN KHỞI CHẠY & VẬN HÀNH (OPERATIONS RUNBOOK)

Hệ thống được đóng gói hoàn toàn bằng Docker Compose để đảm bảo tính nhất quán trên mọi môi trường chạy.

### 4.1. Khởi động và Dừng hệ thống bằng Docker Compose

#### A. Khởi tạo lần đầu
Chạy script cài đặt tự động tại thư mục gốc để cấu hình toàn bộ hệ thống (sinh khóa ngẫu nhiên, tạo các thư mục dữ liệu cục bộ):
```bash
./setup.sh
```
*Lưu ý:* Script sẽ tự động gọi trình Python sinh ngẫu nhiên các mã Fernet và Secret Key cực kỳ an toàn, giúp tránh lỗi cấu hình chéo môi trường.

#### B. Các lệnh vận hành hàng ngày

* **Khởi chạy hệ thống chạy ngầm (Background):**
  ```bash
  docker compose up -d
  ```
  Lệnh này sẽ khởi động toàn bộ 11 container của hệ thống. Bạn có thể kiểm tra trạng thái bằng lệnh:
  ```bash
  docker compose ps
  ```

* **Kiểm tra Log hệ thống thời gian thực:**
  Để theo dõi log của toàn bộ hệ thống hoặc một service cụ thể (ví dụ: clickhouse):
  ```bash
  # Log của tất cả các container
  docker compose logs -f --tail=100
  
  # Chỉ log của ClickHouse
  docker compose logs -f clickhouse
  ```

* **Dừng và Dọn dẹp tài nguyên:**
  Để tắt toàn bộ hệ thống và giải phóng RAM:
  ```bash
  docker compose down
  ```
  Để xóa toàn bộ dữ liệu lưu trữ (Reset sạch cơ sở dữ liệu - Cảnh báo: Mất dữ liệu!):
  ```bash
  docker compose down -v
  ```

---

### 4.2. Vận hành luồng xử lý dữ liệu tự động (Apache Airflow)

Hệ thống sử dụng Apache Airflow để điều phối dòng chảy dữ liệu. 

> 💡 **LƯU Ý QUAN TRỌNG VỀ LUỒNG THU THẬP DỮ LIỆU (LOCAL VS PRODUCTION):**
> * **Trong Môi trường Production (Cloud):** Dữ liệu được cào định kỳ bằng **GitHub Actions** -> đẩy lên **Google Drive** -> Airflow chạy **`dag_sync_gdrive`** để tải về ClickHouse.
> * **Trong Môi trường Cục bộ (Bản cài đặt của bạn):** Do bản cài đặt cục bộ không có GitHub Actions, luồng cào dữ liệu hoạt động **trực tiếp qua API** bằng **`dag_ingest_hourly`** (chạy hoàn toàn tự động hàng giờ, gọi trực tiếp API và ghi thẳng vào ClickHouse cục bộ). **Bạn không cần cấu hình Google Drive OAuth phức tạp ở bản local để cào dữ liệu.**
> * **Vai trò Google Drive ở bản Local:** Ở bản cài đặt local, Google Drive đóng vai trò tùy chọn làm **Nơi lưu trữ snapshot sao lưu cơ sở dữ liệu** (Database Backup) hàng ngày lên đám mây để tránh mất dữ liệu máy chủ vật lý.

```
Ingestion trực tiếp (dag_ingest_hourly chạy mỗi giờ) ────────► Clickhouse Raw
                                                                    │
                                                                    ▼
                                                            dbt Transform (dag_transform tự động kích hoạt)
                                                                    │
                                                                    ▼
                                                            Dashboard & Alerts
```

#### A. Kích hoạt dòng chảy dữ liệu lần đầu (Bootstrap)
Khi bạn khởi chạy hệ thống lần đầu trên ClickHouse trống, hãy thực hiện các bước sau để dựng cấu trúc bảng:

1. Đăng nhập vào **Airflow Webserver** tại `http://localhost:8090` (User: `admin` / Pass: `admin` hoặc pass tự sinh trong `.env`).
2. Tìm kiếm DAG có tên là **`dag_transform`** (DAG chạy các model dbt).
3. **Bật (Unpause) DAG** bằng cách click vào nút gạt đầu dòng.
4. Nhấn nút **Trigger DAG** (biểu tượng Play) ở góc trên bên phải để chạy thủ công lần đầu.
5. DAG này sẽ thực hiện:
   * Tải thư viện phụ thuộc (`dbt deps`).
   * Khởi tạo các bảng từ dữ liệu hạt giống (`dbt seed` - nạp danh mục trạm, tỉnh thành, tọa độ).
   * Biên dịch và chạy toàn bộ 3 lớp model dbt (`dbt run` - Staging, Intermediate, Marts).
   * Kiểm thử chất lượng dữ liệu (`dbt test`).
6. Khi tất cả các hộp tác vụ báo màu **Xanh lá cây**, cơ sở dữ liệu ClickHouse của bạn đã được khởi tạo cấu trúc bảng hoàn hảo!

#### B. Kích hoạt cào dữ liệu thực tế
1. Truy cập trang chủ Airflow, bật (Unpause) DAG **`dag_ingest_hourly`**.
2. DAG này được lập lịch tự động gọi các API cào dữ liệu thời tiết và chất lượng không khí mỗi 15 phút.
3. Nhấn **Trigger DAG** để cào ngay lập tức loạt dữ liệu thực tế đầu tiên. Dữ liệu cào về sẽ tự động đi qua luồng transform dbt để hiển thị lên Dashboard.

---

### 4.3. Xử lý sự cố thường gặp (Troubleshooting)

#### 🚨 Lỗi 1: Clickhouse / Postgres bị crash ngay khi khởi động (Permission Denied)
* **Nguyên nhân:** Hệ điều hành Linux máy chủ chặn container Docker ghi dữ liệu vào thư mục mounts cục bộ.
* **Cách khắc phục:** Cấp quyền ghi rộng cho thư mục dữ liệu trên máy host:
  ```bash
  sudo chown -R 101:101 clickhouse-data
  sudo chmod -R 777 clickhouse-data
  ```

#### 🚨 Lỗi 2: Hệ thống bị tắt ngẫu nhiên (OOM Killed - Hết RAM)
* **Nguyên nhân:** Toàn bộ stack Full Stack (gồm OpenMetadata, Elasticsearch, Clickhouse, Airflow, Prometheus, Grafana) ngốn khoảng **10-12GB RAM**. Nếu máy chủ chỉ có 8GB RAM, hệ điều hành sẽ tự động tắt container lớn nhất (thường là ClickHouse hoặc Elasticsearch).
* **Cách khắc phục:**
  1. Tạo thêm bộ nhớ ảo (Swap space) cho hệ điều hành Linux (hoặc tăng RAM cho WSL2 trên Windows lên tối thiểu 12GB).
  2. Hoặc cấu hình giới hạn RAM trong file `docker-compose.yml` bằng cách giảm `mem_limit` của các container OpenMetadata và ClickHouse xuống thấp hơn.

---

## 5. HƯỚNG DẪN QUẢN TRỊ HỆ THỐNG (SYSTEM ADMINISTRATION)

### 5.1. Quản trị Cơ sở dữ liệu ClickHouse

ClickHouse là kho dữ liệu trung tâm lưu trữ toàn bộ lịch sử chất lượng không khí. 

#### A. Truy cập trực tiếp vào Clickhouse CLI từ terminal máy host:
```bash
docker compose exec -it clickhouse clickhouse-client -u admin --password $(grep CLICKHOUSE_PASSWORD .env | cut -d'=' -f2)
```

#### B. Các câu lệnh truy vấn giám sát hệ thống hữu ích:

* **Kiểm tra kích thước vật lý của các bảng dữ liệu:**
  ```sql
  SELECT 
      table, 
      formatReadableSize(sum(data_compressed_bytes)) AS compressed, 
      formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed, 
      round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio
  FROM system.parts
  WHERE database = 'air_quality' AND active
  GROUP BY table
  ORDER BY sum(data_compressed_bytes) DESC;
  ```

* **Kiểm tra trạng thái cào dữ liệu mới nhất (Freshness Check):**
  ```sql
  SELECT 
      province, 
      station_name, 
      max(ingest_time) as last_ingested_time, 
      now() - max(ingest_time) as latency_seconds
  FROM air_quality.fct_hourly_aqi
  GROUP BY province, station_name
  ORDER BY latency_seconds DESC
  LIMIT 10;
  ```

---

### 5.2. Quản lý Siêu dữ liệu & Lineage với OpenMetadata

OpenMetadata chịu trách nhiệm quản trị chất lượng dữ liệu, xây dựng từ điển dữ liệu (Data Dictionary) và vẽ sơ đồ luồng đi của dữ liệu (Lineage).

1. Truy cập cổng quản trị: `http://localhost:8585` (User/Pass: `admin` / `admin`).
2. **Kiểm tra Sơ đồ Lineage (Luồng dữ liệu):**
   * Vào mục **Explore** -> Chọn tab **Table**.
   * Tìm kiếm bảng `fct_hourly_aqi`.
   * Click vào tab **Lineage** để xem trực quan luồng đi của dữ liệu từ các bảng cào thô (Raw tables) -> dbt staging -> dbt intermediate -> bảng đích Marts.
3. **Giám sát chất lượng dữ liệu (Data Quality):**
   * Click vào tab **Profiler & Data Quality** tại bất kỳ bảng nào để xem số lượng bản ghi lỗi, tỉ lệ giá trị Null, và kết quả chạy của 85+ dbt tests tự động mỗi ngày.

---

### 5.3. Quản trị Dashboard và Cảnh báo với Grafana

Grafana hiển thị biểu đồ hiệu năng hệ thống và kích hoạt gửi tin nhắn cảnh báo tới Telegram của kỹ sư trực ca (On-call Engineer).

#### A. Truy cập giao diện giám sát
Truy cập: `http://localhost:3000` (Tài khoản: `admin` / `admin`).
Vào phần **Dashboards** để xem 2 dashboard dựng sẵn cực kỳ chuyên nghiệp:
1. **Clickhouse Metrics Dashboard:** Đo lường số lượng Queries/giây, RAM CPU Clickhouse tiêu thụ, Tốc độ ghi đĩa.
2. **Air Quality Alerting Dashboard:** Theo dõi các trạm đo có AQI chạm ngưỡng nguy hại.

#### B. Cách tùy biến đích nhận cảnh báo Telegram
Mặc định hệ thống sử dụng file cấu hình tự động nạp (provisioning). Nếu bạn muốn đổi nhóm chat Telegram nhận tin nhắn:
1. Mở menu bên trái, chọn **Alerting** -> click **Contact points**.
2. Tìm contact point tên là **`Telegram Air Quality Alerts`**. Click vào biểu tượng chỉnh sửa (bút chì).
3. Tại ô **Chat ID**, điền mã ID nhóm chat Telegram mới của bạn (Lấy ở mục 2.5).
4. Click **Test** ở góc phải để kiểm tra xem bot Telegram có gửi tin nhắn chào mừng vào nhóm ngay lập tức không.
5. Click **Save contact point**.

---
*Tài liệu này được biên soạn độc lập giúp chuẩn hóa quy trình vận hành dự án. Chúc bạn có những trải nghiệm tuyệt vời với Vietnam Air Quality Data Platform!*
