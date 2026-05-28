# HƯỚNG DẪN TRIỂN KHAI VÀ ĐÓNG GÓI NỀN TẢNG (DEPLOYMENT GUIDE)
## Nền Tảng Dữ Liệu Chất Lượng Không Khí Việt Nam (Vietnam Air Quality Data Platform)

Tài liệu này cung cấp hướng dẫn chi tiết cách đóng gói, cài đặt và vận hành hệ thống chất lượng không khí trên máy tính mới với **2 phiên bản tùy chọn**:

1. **Phiên bản Trải nghiệm Siêu nhẹ (Experience Edition):** Chỉ gồm ClickHouse + Streamlit + Grafana. Chạy offline, sử dụng dữ liệu tĩnh có sẵn từ máy bạn gửi đi, không cần điền API key nhạy cảm, cực kỳ nhẹ và nhanh.
2. **Phiên bản Đầy đủ (Full Stack Edition):** Toàn bộ stack công nghệ gồm Airflow 3, Postgres, OpenMetadata, Elasticsearch, Grafana và hệ thống cào dữ liệu thực tế tự động.

---

## 🛠️ YÊU CẦU HỆ THỐNG CHUNG
Trước khi cài đặt bất kỳ phiên bản nào, máy tính mục tiêu cần đáp ứng các điều kiện tối thiểu sau:
* **Hệ điều hành:** Linux (Ubuntu/Debian), macOS, hoặc Windows (bắt buộc thông qua WSL2).
* **Công cụ:** Đã cài đặt **Docker** và **Docker Compose**.
* **Đường truyền mạng:** Ổn định để tải các Docker image lần đầu.

---

## 📂 PHẦN I: PHIÊN BẢN TRẢI NGHIỆM SIÊU NHẸ (EXPERIENCE EDITION)
*Phù hợp cho đối tác, khách hàng hoặc tester chỉ cần xem báo cáo phân tích, bản đồ và biểu đồ trực quan mà không cần cào dữ liệu mới.*

### 1. Đặc điểm nổi bật
* **Không cần API Token:** Toàn bộ API keys như OpenWeather, TomTom, Telegram... được lược bỏ hoàn toàn, đảm bảo an toàn tuyệt đối.
* **Sẵn có dữ liệu thực (Siêu nhẹ ~25MB):** Sử dụng công nghệ xuất dữ liệu logic Parquet giúp thu gọn kích thước từ 1.7GB xuống chỉ còn dưới 30MB. Cơ chế tự động import sẽ tự nạp dữ liệu sạch vào máy khách khi khởi chạy container lần đầu.
* **Nhẹ & Tiết kiệm tài nguyên:** Chỉ cần trống tối thiểu **4GB RAM**. Cắt bỏ Airflow, Postgres, OpenMetadata giúp hệ thống khởi động tức thì.
* **Bảo vệ mã nguồn:** Trang **Ask Data** (AI Search) được tự động giới hạn và yêu cầu người dùng tự nhập Groq Key của họ nếu muốn trải nghiệm tính năng AI.

### 2. Cách đóng gói từ máy của bạn
Mở terminal tại thư mục dự án và chạy script đóng gói tự động:
```bash
./setup_experience.sh
```
Hệ thống sẽ dừng docker tạm thời, dọn dẹp cache và nén tất cả các tệp cần thiết thành một tệp duy nhất là **`vietnam_aqi_experience.tar.gz`**.

### 3. Hướng dẫn cài đặt trên máy mới
1. Gửi file `vietnam_aqi_experience.tar.gz` sang máy mới.
2. Giải nén file:
   ```bash
   tar -xzvf vietnam_aqi_experience.tar.gz
   cd staging_experience
   ```
3. Khởi chạy toàn bộ hệ thống bằng một lệnh duy nhất:
   ```bash
   docker compose up -d
   ```
4. Truy cập giao diện trải nghiệm trên trình duyệt:
   * **Streamlit Dashboard (Giao diện chính):** [http://localhost:8501](http://localhost:8501)
   * **Grafana Dashboards:** [http://localhost:3000](http://localhost:3000) (Tài khoản: `admin` / Mật khẩu: `admin`)

---

## 🚀 PHẦN II: PHIÊN BẢN ĐẦY ĐỦ (FULL STACK EDITION)
*Phù hợp cho lập trình viên cần phát triển tiếp, hoặc chạy sản xuất (production) với hệ thống tự động cào dữ liệu liên tục.*

### 1. Đặc điểm nổi bật
* Hoạt động tự động cào dữ liệu thời gian thực mỗi 15 phút qua Airflow.
* Quản trị dữ liệu toàn diện với OpenMetadata & Elasticsearch.
* Giám sát hạ tầng hệ thống chặt chẽ với Prometheus & Grafana.
* Yêu cầu tài nguyên máy tính trống tối thiểu: **16GB RAM** (Khuyến nghị **32GB RAM**).

### 2. Hướng dẫn cài đặt trên máy mới
1. Clone mã nguồn Git đầy đủ sang máy mới.
2. Mở terminal tại thư mục dự án và chạy script khởi tạo:
   ```bash
   ./setup_full.sh
   ```
3. Script sẽ tự động sinh tệp `.env` bảo mật và tự động tạo ngẫu nhiên các khóa bí mật cực kỳ an toàn (`AIRFLOW__CORE__FERNET_KEY`, `CLICKHOUSE_PASSWORD`...).
4. Mở tệp `.env` mới tạo bằng công cụ soạn thảo và điền các API Token cá nhân của bạn để kích hoạt cào dữ liệu:
   * `OPENWEATHER_API_TOKEN`
   * `TOMTOM_API_KEY`
   * `WAQI_TOKEN`
   * `GROQ_API_KEY` (Cho tính năng hỏi đáp AI)
5. Nhấn **[Enter]** ở cửa sổ terminal để script tự động build các Docker image tùy biến và khởi chạy toàn bộ 14+ container.
6. Địa chỉ truy cập các dịch vụ:
   * **Streamlit Dashboard:** [http://localhost:8501](http://localhost:8501)
   * **Airflow Webserver:** [http://localhost:8090](http://localhost:8090) (Tài khoản: `admin` / Mật khẩu: `admin`)
   * **OpenMetadata Catalog:** [http://localhost:8585](http://localhost:8585)
   * **Grafana Dashboards:** [http://localhost:3000](http://localhost:3000) (Tài khoản: `admin` / Mật khẩu: `admin`)

---

## ⚙️ PHẦN III: HƯỚNG DẪN CẤU HÌNH VÀ VẬN HÀNH BẢN ĐẦY ĐỦ (FULL STACK OPERATIONS)
*Bản Full Stack khi cài đặt ban đầu sẽ hoàn toàn trống dữ liệu. Dưới đây là các bước chi tiết để cấu hình API và kích hoạt dòng chảy dữ liệu tự động lần đầu tiên.*

### 1. Chi tiết cấu hình các API Key trong tệp `.env`
Để hệ thống tự động cào dữ liệu thực tế thời gian thực, lập trình viên cần điền các tham số sau vào tệp `.env`:
* **`OPENWEATHER_API_TOKEN`:** Lấy khóa API miễn phí từ [OpenWeather](https://openweathermap.org/api) (dùng cho dữ liệu thời tiết và ô nhiễm không khí tại 62 tỉnh thành).
* **`TOMTOM_API_KEY`:** Lấy khóa API miễn phí từ [TomTom Developer Portal](https://developer.tomtom.com/) (dùng cho dữ liệu mật độ giao thông tại các nút điểm).
* **`WAQI_TOKEN`:** Lấy token miễn phí từ [AQICN Data Platform](https://aqicn.org/data-platform/token/) (dùng cho dữ liệu từ ~540 trạm quan trắc).
* **`GROQ_API_KEY`:** Khóa API của Groq (dùng cho tính năng hỏi đáp tự nhiên Ask Data).
* **`TELEGRAM_AQ_BOT_TOKEN` & `TELEGRAM_AQ_CHAT_ID`:** Token của Telegram Bot để gửi cảnh báo chất lượng không khí vượt ngưỡng trực tiếp vào nhóm chat.

### 2. Quy trình Vận hành và Kích hoạt Dữ liệu Lần đầu
Sau khi chạy `./setup_full.sh` và các container đã hoạt động:

#### 🚀 Bước A: Khởi tạo Cấu trúc Bảng & Dữ liệu Hạt giống (dbt seed)
1. Truy cập giao diện quản trị **Airflow Webserver** tại [http://localhost:8090](http://localhost:8090) (Tài khoản: `admin` / Mật khẩu: `admin` hoặc mật khẩu ngẫu nhiên trong `.env`).
2. Tìm DAG có tên là **`dag_transform`** (đây là DAG chịu trách nhiệm chạy dbt).
3. Bật (Unpause) DAG này và click nút **Trigger** ở góc trên cùng bên phải.
4. DAG sẽ chạy chuỗi tác vụ: `dbt deps` -> `dbt seed` (để tự động nạp các tọa độ trạm, danh mục chất ô nhiễm, đơn vị hành chính) -> `dbt run` -> `dbt test`. Sau khi DAG báo xanh, toàn bộ cấu trúc cơ sở dữ liệu trên ClickHouse đã sẵn sàng!

#### 📥 Bước B: Chạy Ingestion DAGs để Thu thập Dữ liệu Thực tế
1. Quay lại trang chủ Airflow, bật (Unpause) DAG **`dag_ingest_hourly`**.
2. DAG này được lập lịch chạy mỗi 15 phút một lần để gọi song song các API cào dữ liệu chất lượng không khí của AQI.in và OpenWeather.
3. **Chạy thủ công lần đầu:** Bạn có thể click **Trigger** DAG này ngay lập tức để nạp loạt dữ liệu thực tế đầu tiên vào ClickHouse.
4. (Tùy chọn) Bật DAG **`dag_metadata_update`** để tự động cập nhật danh sách các trạm đo hàng ngày.

#### 📊 Bước C: Xác minh Dòng chảy Dữ liệu trên Dashboard
1. Truy cập **Streamlit Dashboard** tại [http://localhost:8501](http://localhost:8501).
2. Các trang **Overview**, **Pollutants**, **Historical Trend** sẽ tự động hiển thị dữ liệu thời gian thực vừa cào về một cách sinh động.
3. Truy cập **OpenMetadata** tại [http://localhost:8585](http://localhost:8585) để kiểm tra luồng dữ liệu (lineage) từ ClickHouse sang dbt.

---

## 🛑 HƯỚNG DẪN XỬ LÝ LỖI THƯỜNG GẶP (TROUBLESHOOTING)

### 1. Lỗi phân quyền ghi dữ liệu (Permission Denied)
* **Triệu chứng:** Container ClickHouse hoặc Postgres bị crash khi khởi chạy lần đầu và log báo lỗi phân quyền ghi vào thư mục `/var/lib/...`.
* **Cách sửa:** Chạy lệnh phân quyền trên máy Linux của bạn:
  ```bash
  sudo chown -R 101:101 clickhouse-data
  sudo chmod -R 777 clickhouse-data
  ```

### 2. Tràn bộ nhớ RAM khiến container bị ép tắt (OOM Killed)
* **Triệu chứng:** Một số dịch vụ lớn như ClickHouse hoặc Elasticsearch tự động bị tắt mà không rõ nguyên nhân.
* **Cách sửa:** Tăng dung lượng RAM swap cho Docker Desktop/WSL2 hoặc hạ giới hạn `mem_limit` trong tệp `docker-compose.yml` xuống mức thấp hơn (Ví dụ giảm ClickHouse xuống `2g` hoặc `3g`).

---

## 🚢 PHẦN IV: HƯỚNG DẪN CI/CD & QUẢN TRỊ PHIÊN BẢN (CI/CD & VERSION RELEASES)

Hệ thống tích hợp quy trình **CI/CD Quản trị Phiên bản (Version Release Management)** tự động hóa cao bằng GitHub Actions. Quy trình này tách biệt giữa quá trình kiểm thử mã nguồn hàng ngày (CI) và quá trình đóng gói phát hành Docker Images lên Docker Hub (CD).

### 1. Cấu hình GitHub Secrets ban đầu
Để kích hoạt tính năng CD đẩy ảnh Docker lên Docker Hub, quản trị viên cần khai báo 2 **Repository Secrets** trong phần cài đặt của GitHub Repository (`Settings > Secrets and variables > Actions`):
1. **`DOCKERHUB_USERNAME`**: Tên tài khoản Docker Hub của bạn (Ví dụ: `nguyennhuong`).
2. **`DOCKERHUB_TOKEN`**: Khóa Personal Access Token (PAT) được sinh từ Docker Hub có quyền write.

*(Lưu ý: Nếu không cấu hình các secret này, pipeline vẫn sẽ chạy build thử nghiệm để kiểm tra lỗi biên dịch mã nguồn nhưng sẽ tự động bỏ qua bước Push lên registry mà không gây lỗi đỏ workflow).*

### 2. Kích hoạt Tự động qua Git Tags (Gắn nhãn Phiên bản)
Mỗi khi bạn muốn đóng gói và công bố một phiên bản chính thức, hãy tạo Git Tag có định dạng `v*` và đẩy lên GitHub:
```bash
# Tạo thẻ phiên bản v1.0.0
git tag v1.0.0

# Đẩy thẻ tag lên GitHub để kích hoạt pipeline
git push origin v1.0.0
```
* **Cách hoạt động:** Pipeline sẽ tự động kích hoạt, build song song cả 4 images cốt lõi (Airflow, Streamlit Dashboard, Text-to-SQL, và Stats Exporter) bằng Docker Buildx và đẩy chúng lên registry:
  - **Phiên bản Ổn định (Stable Release)**: Nếu tag phát hành không chứa ký tự gạch ngang `-` (Ví dụ: `v1.0.0`), ảnh sẽ được đẩy lên với cả hai nhãn là `:1.0.0` và `:latest`.
  - **Phiên bản Thử nghiệm (Pre-release)**: Nếu tag phát hành có chứa ký tự gạch ngang `-` theo chuẩn SemVer (Ví dụ: `v1.0.0-beta`, `v2.0.0-rc1`), ảnh sẽ **chỉ được đẩy lên với nhãn tương ứng đó** và hoàn toàn **bỏ qua** việc ghi đè lên nhãn `:latest` ổn định hiện tại. Điều này giúp ngăn ngừa việc các bản thử nghiệm đè lên bản ổn định trong môi trường production.

### 3. Kích hoạt Thủ công linh hoạt (Manual Run - workflow_dispatch)
Nếu muốn phát hành nhanh hoặc chỉ cập nhật một phần của hệ thống, bạn có thể chạy thủ công từ giao diện của GitHub Actions:
1. Truy cập tab **Actions** trên GitHub và chọn **Docker Release Pipeline**.
2. Nhấn nút **Run workflow** ở góc phải.
3. Điền các tham số trong form hiển thị:
   - **`Version Tag`**: Tên tag phiên bản muốn gán (ví dụ: `latest`, `dev`, `1.2.0-beta`).
   - Các nút tích chọn để bật/tắt build cho từng Image (**Airflow**, **Dashboard**, **Text-to-SQL**, **Stats Exporter**). 
4. *Ví dụ: Nếu bạn chỉ cập nhật giao diện mà không sửa đổi dbt hay AI, hãy bỏ tích 3 thành phần kia và chỉ tích chọn "Build & Push Streamlit Dashboard Image". Job sẽ chạy cực nhanh và chỉ cập nhật duy nhất Dashboard trên Docker Hub!*

### 4. Đóng gói Offline không cần ClickHouse (`--refresh-seeds`)
Mặc định, các script đóng gói cục bộ (`./deployments/package_full.sh` và `./deployments/package_experience.sh`) chạy hoàn toàn **offline**, không cần chạy container ClickHouse hay trích xuất dữ liệu trực tiếp, giúp việc build bundle nhanh chóng (<5 giây) nhờ sử dụng thư mục Parquet seeds có sẵn (`clickhouse-seeds/`).

Nếu bạn muốn cập nhật dữ liệu mẫu 14 ngày mới nhất từ cơ sở dữ liệu Clickhouse cục bộ đang chạy vào gói nén, hãy thêm tham số `--refresh-seeds`:
```bash
# Đóng gói Full Stack và đồng thời trích xuất dữ liệu Clickhouse mới
./deployments/package_full.sh --refresh-seeds

# Đóng gói bản Trải nghiệm siêu nhẹ và cập nhật dữ liệu
./deployments/package_experience.sh --refresh-seeds
```

### 5. Tùy chọn nạp dữ liệu mẫu khi Cài đặt (`LOAD_SAMPLE_DATA`)
Khi chạy script khởi tạo ban đầu `./setup.sh` (hoặc `./setup_full.sh`), hệ thống sẽ hỏi lựa chọn:
> *Bạn có muốn tự động nạp dữ liệu mẫu lịch sử 14 ngày vào ClickHouse không? (Y/n)*

Lựa chọn này sẽ cập nhật biến cấu hình **`LOAD_SAMPLE_DATA`** trong tệp `.env`:
* **`LOAD_SAMPLE_DATA=true`**: Khi khởi chạy container ClickHouse lần đầu, script `/docker-entrypoint-initdb.d/02-import.sh` sẽ tự động quét và nạp siêu tốc toàn bộ dữ liệu mẫu Parquet lịch sử vào database. Hệ thống có sẵn dữ liệu phân tích ngay lập tức.
* **`LOAD_SAMPLE_DATA=false`**: ClickHouse sẽ khởi động sạch hoàn toàn (trống trơn). Phù hợp nếu bạn muốn hệ thống lưu trữ độc quyền dữ liệu thực tế tự cào bằng API mà không bị lẫn dữ liệu mẫu.

