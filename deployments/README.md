# Thư Mục Triển Khai & Đóng Gói (Deployments)
## Nền Tảng Dữ Liệu Chất Lượng Không Khí Việt Nam (Vietnam Air Quality Data Platform)

Thư mục này chứa toàn bộ cấu hình, tập lệnh khởi tạo, và công cụ đóng gói phục vụ quá trình triển khai hệ thống chất lượng không khí trên nhiều môi trường khác nhau.

---

## 📂 Cấu Trúc Thư Mục Deployments

```text
deployments/
├── full/
│   ├── docker-compose.yml       # Cấu hình Docker Compose bản Full Stack đầy đủ
│   └── setup.sh                 # Tập lệnh setup riêng của bản Full Stack
├── experience/
│   └── docker-compose.yml       # Cấu hình Docker Compose bản Trải nghiệm tối giản
├── package_deploy.sh            # Script đóng gói hợp nhất cả 2 phiên bản (<100MB ZIP)
├── DEPLOY_README.md             # File README mẫu sẽ được copy vào gói nén phân phối
├── HUONG_DAN_VAN_HANH.md        # Hướng dẫn vận hành chi tiết các API Key & giám sát
└── README.md                    # Tài liệu này (Giải thích thư mục deployments)
```

---

## 🛠️ Chi Tiết Các Thành Phần

### 1. `package_deploy.sh` (Tập lệnh Đóng gói Hợp nhất)
Đây là tập lệnh chính được sử dụng bởi Lập trình viên để đóng gói dự án. Script này sẽ:
- Dọn dẹp các tài nguyên cache, Docker đang chạy.
- Khởi tạo thư mục staging tạm thời và gom tất cả cấu hình Docker Compose (`full` và `experience`), hạt giống dữ liệu (`clickhouse-seeds`), cấu hình giám sát (`monitoring`), mã nguồn giao diện Streamlit (`python_jobs/dashboard`).
- Tự động tích hợp tập lệnh cài đặt đa nền tảng tương tác `setup.sh` và tài liệu cài đặt `README.md` (từ `DEPLOY_README.md`).
- Nén tất cả thành một tệp tin duy nhất là **`vietnam_aqi_platform.zip`** ở thư mục gốc của dự án với dung lượng siêu tối ưu **~85 MB** (thoải mái dưới ngưỡng 100 MB).

### 2. `DEPLOY_README.md` (Tài liệu Cài đặt Phân phối)
Tệp Markdown này lưu trữ nội dung hướng dẫn cài đặt đa nền tảng chi tiết. Khi lập trình viên chạy tập lệnh đóng gói, tệp này sẽ tự động được đổi tên thành `README.md` và đặt tại thư mục gốc của gói nén `.zip`, hướng dẫn chi tiết cách cài đặt trên **Windows (WSL2, Git Bash, PowerShell), Linux và macOS**.

### 3. `HUONG_DAN_VAN_HANH.md` (Cẩm nang Vận hành)
Tập tài liệu hướng dẫn vận hành chi tiết bằng Tiếng Việt dành cho quản trị viên hoặc lập trình viên phát triển hệ thống:
- Hướng dẫn lấy Token và cấu hình các dịch vụ API: OpenWeather, TomTom Map, WAQI API, Groq AI.
- Hướng dẫn tích hợp cảnh báo Telegram Bot (nhóm chat cảnh báo chất lượng không khí & nhóm chat cảnh báo hệ thống hạ tầng).
- Quy trình kích hoạt luồng dữ liệu dbt và Ingestion lần đầu trên Apache Airflow 3.

---

## 🚀 Hướng Dẫn Dành Cho Lập Trình Viên

### 1. Đóng gói phân phối thông thường (sử dụng dữ liệu hạt giống sẵn có)
Khi bạn muốn tạo gói phân phối mới mà không cần cập nhật dữ liệu mẫu lịch sử 14 ngày, chạy lệnh sau ở thư mục gốc của dự án:
```bash
./deployments/package_deploy.sh
```

### 2. Đóng gói kèm cập nhật dữ liệu hạt giống mới từ ClickHouse
Nếu bạn đã cào thêm dữ liệu mới và muốn trích xuất dữ liệu ClickHouse 14 ngày gần nhất để làm dữ liệu mẫu tĩnh mới cho gói đóng gói, hãy chạy lệnh:
```bash
./deployments/package_deploy.sh --refresh-seeds
```
*(Lưu ý: Lệnh này yêu cầu ClickHouse cục bộ của bạn đang chạy và có dữ liệu thực tế).*
