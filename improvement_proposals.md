# 🏗️ Đề xuất cải tiến — Vietnam Air Quality Data Platform

> Phân tích dựa trên nghiên cứu toàn bộ codebase (Airflow DAGs, Python ingestion jobs, dbt models, Text-to-SQL service, Docker infrastructure, monitoring stack).
> Ngày: 2026-05-05

---

## Tổng quan kiến trúc hiện tại

```
┌─────────────────────────────────────────────────────────────┐
│  Sources: AQI.in (540 stations) + OpenWeather + TomTom      │
│       ↓ Airflow (dag_ingest_hourly) — schedule=None         │
│  ClickHouse [raw_*] → dbt [stg_* → int_* → fct_*/dm_*]     │
│       ↓ Airflow (dag_transform) — triggered by ingest       │
│  Dashboard (Streamlit) + Text-to-SQL (FastAPI/Vanna/Groq)   │
│  Monitoring: Prometheus + Grafana + Telegram alerts         │
│  Metadata: OpenMetadata 1.12.4 + Elasticsearch              │
└─────────────────────────────────────────────────────────────┘
```

**Điểm mạnh nổi bật:**
- Kiến trúc medallion (raw → staging → intermediate → marts) rõ ràng
- SQL validator đa tầng cho Text-to-SQL (keyword check + AST parse + allowlist)
- Monitoring stack đầy đủ (Prometheus/Grafana/Telegram)
- Preview-token pattern bảo vệ lệnh execute trong Text-to-SQL
- dbt incremental models với `ReplacingMergeTree` phù hợp ClickHouse

---

## 🔴 Nhóm 1: Bảo mật & Secrets Management (Ưu tiên Cao)

### 1.1 — Credentials bị hardcode trong docker-compose.yml

**Vấn đề phát hiện:**

```yaml
# docker-compose.yml line 506
- GF_SECURITY_ADMIN_PASSWORD=admin123456
# line 506 — cũng xuất hiện làm default trong nhiều env vars
'CLICKHOUSE_PASSWORD': os.environ.get('CLICKHOUSE_PASSWORD', 'admin123456')
```

Default password `admin123456` lặp lại trong cả `docker-compose.yml`, `dag_ingest_hourly.py`, `dag_transform.py`, và `dag_weekly_report.py`. Nếu `.env` không được set, hệ thống chạy với password yếu mặc định.

**Đề xuất:**
1. Xóa toàn bộ default password fallback trong Python code — raise `EnvironmentError` nếu không có env var
2. Thêm validation script `scripts/check-env.sh` kiểm tra tất cả required secrets trước khi `docker compose up`
3. Thêm vào `.env.example` comment rõ ràng: _"NEVER use these values in production"_
4. Dùng Docker Secrets hoặc Vault cho production deployment

```python
# Thay thế pattern này:
CLICKHOUSE_PASSWORD = os.environ.get('CLICKHOUSE_PASSWORD', 'admin123456')  # ❌

# Bằng:
CLICKHOUSE_PASSWORD = os.environ['CLICKHOUSE_PASSWORD']  # ✅ Fail fast nếu thiếu
```

### 1.2 — AIRFLOW__CORE__FERNET_KEY để trống

**Vấn đề:**

```yaml
# docker-compose.yml line 67
- AIRFLOW__CORE__FERNET_KEY=   # ❌ Trống — không mã hóa connections
```

Fernet key trống có nghĩa tất cả Airflow connections (API keys, passwords) lưu dưới dạng plaintext trong PostgreSQL metadata database.

**Đề xuất:**
```bash
# Tạo key và lưu vào .env
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 1.3 — Airflow Webserver dùng LocalExecutor mà không có worker isolation

**Vấn đề:** `LocalExecutor` chạy tất cả tasks trong cùng process pool — một task lỗi/memory leak ảnh hưởng toàn bộ scheduler.

**Đề xuất:** Cân nhắc `CeleryExecutor` với Redis nếu scale up, hoặc thêm task timeout:
```python
default_args = {
    'execution_timeout': timedelta(minutes=30),  # Thêm vào mọi DAG
}
```

---

## 🔴 Nhóm 2: Hiệu năng & Độ tin cậy Pipeline (Ưu tiên Cao)

### 2.1 — `dag_ingest_hourly` dùng `schedule=None` — không chạy tự động

**Vấn đề nghiêm trọng:**

```python
# dag_ingest_hourly.py line 49
schedule=None,  # ❌ DAG không tự trigger!
```

DAG được comment là "runs every hour (0 * * * *)" nhưng `schedule=None` có nghĩa nó chỉ chạy khi trigger thủ công hoặc qua API. Không có DAG nào khác trigger nó theo lịch hourly.

**Đề xuất:** Sửa ngay:
```python
schedule='0 * * * *',  # Hourly như thiết kế
# hoặc dùng cron preset:
schedule='@hourly',
```

### 2.2 — `dbt deps` chạy mỗi lần transform — tốn thời gian

**Vấn đề:**

```python
# dag_transform.py — dbt_deps() luôn chạy đầu mỗi chu kỳ
check_clickhouse >> check_dbt >> deps >> seed >> staging >> ...
```

`dbt deps` tải packages từ internet mỗi giờ — latency ~30-60s và risk nếu registry down.

**Đề xuất:** Chỉ chạy `dbt deps` khi packages thay đổi:
```python
@task
def dbt_deps_if_needed():
    """Chỉ chạy deps nếu packages.yml thay đổi."""
    lock_file = Path(DBT_PROJECT_DIR) / 'package-lock.yml'
    packages_file = Path(DBT_PROJECT_DIR) / 'packages.yml'
    if not lock_file.exists() or lock_file.stat().st_mtime < packages_file.stat().st_mtime:
        # Run deps
        ...
```

Hoặc sử dụng `DBT_PACKAGES_INSTALL_PATH` đã được cấu hình + chỉ chạy deps trong DAG riêng triggered khi deploy.

### 2.3 — Task `run_traffic_processing` tính giờ Vietnam từ `data_interval_start`

**Vấn đề tinh tế:**

```python
# dag_ingest_hourly.py lines 118-120
data_interval_start = context.get('data_interval_start')
hour = (data_interval_start.hour + 7) % 24 if data_interval_start else (datetime.now().hour + 7) % 24
```

Khi DAG triggered thủ công (không có `data_interval_start`), rơi vào `datetime.now()` — giờ của Airflow worker container (có thể UTC), không phải giờ Việt Nam thực tế.

**Đề xuất:**
```python
from zoneinfo import ZoneInfo
vn_now = datetime.now(ZoneInfo('Asia/Ho_Chi_Minh'))
hour = vn_now.hour
```

### 2.4 — `subprocess.run()` với `shell=True` trong mọi Airflow task

**Vấn đề:** Tất cả ingestion tasks dùng:
```python
result = subprocess.run(cmd, shell=True, env=env, capture_output=True, text=True)
```

`shell=True` với f-string interpolation có nguy cơ shell injection nếu env vars bị compromised. Hơn nữa, không có process group tracking — child processes có thể orphan khi task bị cancel.

**Đề xuất:** Dùng list form:
```python
result = subprocess.run(
    ['python', 'jobs/aqiin/ingest_measurements.py', '--mode', 'incremental'],
    env=env,
    capture_output=True,
    text=True,
    timeout=600,
)
```

### 2.5 — `update_ingestion_control` ghi hardcode `records_ingested=0`

**Vấn đề:**
```python
# dag_ingest_hourly.py line 147
_update(source='dag_ingest_hourly', records_ingested=0, success=True)  # ❌ Luôn 0
```

Control table không phản ánh số records thực tế — mất khả năng monitoring data volume.

**Đề xuất:** Trả về số records từ mỗi ingest task và aggregate:
```python
@task
def update_ingestion_control(aqiin_count: int, ow_count: int, traffic_count: int):
    total = (aqiin_count or 0) + (ow_count or 0) + (traffic_count or 0)
    _update(source='dag_ingest_hourly', records_ingested=total, success=True)
```

---

## 🟡 Nhóm 3: Chất lượng Dữ liệu & dbt Tests (Ưu tiên Trung bình)

### 3.1 — `fct_air_quality_province_level_hourly` dùng `select *` từ final CTE

**Vấn đề:**
```sql
-- fct_air_quality_province_level_hourly.sql line 84
select * from final
```

`SELECT *` trong dbt mart model là anti-pattern — khi schema thay đổi ở upstream, mart tự động thêm/bỏ columns mà không có explicit contract.

**Đề xuất:** Enumerate columns rõ ràng và thêm dbt contracts:
```yaml
# _mart_core__models.yml
models:
  - name: fct_air_quality_province_level_hourly
    config:
      contract:
        enforced: true
    columns:
      - name: datetime_hour
        data_type: datetime
        constraints:
          - type: not_null
```

### 3.2 — `dm_traffic_pollution_correlation_daily` có `ward_code` là empty string

**Vấn đề phát hiện:**
```sql
-- dm_traffic_pollution_correlation_daily.sql line 83
'' as ward_code,  -- ❌ Placeholder không có ý nghĩa
```

Column tồn tại trong schema nhưng luôn rỗng — gây nhầm lẫn cho người dùng Text-to-SQL.

**Đề xuất:** Xóa column hoặc document rõ là deprecated:
```sql
-- Deprecated: không có dữ liệu ward-level cho traffic correlation
-- CAST(NULL AS Nullable(String)) as ward_code,
```

### 3.3 — Hardcoded province classification trong analytics mart

**Vấn đề:**
```sql
-- dm_traffic_pollution_correlation_daily.sql lines 35-37
when province IN ('Hà Nội', 'Hồ Chí Minh', 'Đà Nẵng', ...) then 'Urban'
when province IN ('Bình Dương', 'Đồng Nai', ...) then 'Industrial'
else 'Rural'
```

Logic business được nhúng trong SQL — khó maintain, duplicate với các model khác, và sai nếu tỉnh thay đổi phân loại.

**Đề xuất:** Tạo dbt seed file:
```csv
-- seeds/province_classification.csv
province,location_type,region_3,region_8
Hà Nội,Urban,North,Red River Delta
Hồ Chí Minh,Urban,South,Southeast
...
```

Rồi `ref('province_classification')` trong SQL.

### 3.4 — `dag_transform.py` chạy `dbt test` sau marts — fail sẽ block downstream

**Vấn đề thiết kế:**
```python
# dag_transform.py line 400
staging >> intermediate >> marts >> test >> docs >> patch >> stats >> completion
```

Nếu `dbt test` fail, toàn bộ pipeline dừng — kể cả patch artifacts và stats. Nên phân biệt test "blocking" vs "non-blocking".

**Đề xuất:**
```python
# Chạy generic tests (not_null, unique) sau mỗi layer
staging >> staging_tests >> intermediate >> intermediate_tests >> marts >> mart_tests
# Mart tests fail → alert nhưng không block docs generation
```

---

## 🟡 Nhóm 4: Text-to-SQL Service (Ưu tiên Trung bình)

### 4.1 — `VannaRuntime` re-train mỗi lần cold start

**Vấn đề:**
```python
# vanna_runtime.py lines 123-127
def _get_vanna_client(self) -> Any:
    if self._vanna_client is None:
        self._vanna_client = self._create_vanna_client()
        self._train_vanna_client(self._vanna_client)  # Training mỗi container restart
```

Vanna dùng ChromaDB in-memory — mất toàn bộ training data khi container restart. Mỗi cold start phải train lại từ đầu (~30-60s).

**Đề xuất:**
- Persist ChromaDB volume: `./chroma-data:/app/chroma`
- Hoặc chuyển sang ChromaDB server mode với dedicated container
- Thêm health check endpoint `/ready` trả về training status

### 4.2 — `PreviewStore` in-memory — không horizontal scale

**Vấn đề:**
```python
# app.py lines 45-46
self._records: dict[str, PreviewRecord] = {}  # ❌ In-memory only
```

Preview tokens mất khi container restart. Nếu load balancer route `/ask` và `/execute` tới các replicas khác nhau, token validation sẽ fail.

**Đề xuất:** Dùng Redis cho token store:
```python
import redis

class RedisPreviewStore:
    def __init__(self, redis_url: str, ttl_seconds: int = 900):
        self.redis = redis.from_url(redis_url)
        self.ttl_seconds = ttl_seconds
    
    def issue(self, sql: str) -> str:
        token = self._generate_token(sql)
        self.redis.setex(token, self.ttl_seconds, sql)
        return token
```

### 4.3 — `_extract_sql_statement` quá phức tạp với regex fallback chains

**Vấn đề:**
```python
# vanna_runtime.py lines 137-156
fenced_blocks = re.findall(r"```(?:sql)?\s*(.*?)```", ...)
# ... 3 tầng fallback regex
```

Logic extract SQL từ LLM response rất fragile — một thay đổi nhỏ trong format output của Groq/Vanna có thể break toàn bộ. Hiện không có test coverage cho các edge cases.

**Đề xuất:**
1. Thêm unit tests với fixture responses từ Groq API thực tế
2. Log raw LLM response vào structured log để debug
3. Thêm metric counter: `sql_extraction_method` (fenced/regex/fallback/failed)

### 4.4 — Không có rate limiting trên `/ask` endpoint

**Vấn đề:** FastAPI app không có rate limiting — một user có thể spam `/ask` gây tốn API quota của Groq.

**Đề xuất:**
```python
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)

@app.post("/ask")
@limiter.limit("10/minute")
def ask(request: AskRequest, ...):
    ...
```

---

## 🟡 Nhóm 5: Kiến trúc dbt & Maintainability (Ưu tiên Trung bình)

### 5.1 — Typo trong tên thư mục: `dbt_tranform` (thiếu chữ 's')

**Vấn đề nhỏ nhưng gây confusion:**
```
/dbt/dbt_tranform/  ← thiếu 's' trong 'transform'
```

Được reference khắp nơi trong docker-compose.yml (~20 lần), DAG files, README — sẽ rất phức tạp để sửa nhưng nên ghi nhận để fix trong version mới.

### 5.2 — `order_by` và `unique_key` không nhất quán trong incremental models

**Vấn đề:**
```sql
-- fct_air_quality_province_level_hourly.sql
{{ config(
    engine='ReplacingMergeTree',
    unique_key='(province, datetime_hour)',   -- unique_key có datetime_hour
    order_by='(province, date)',              -- ❌ order_by chỉ có date, thiếu datetime_hour
) }}
```

`ReplacingMergeTree` trong ClickHouse merge dựa trên `ORDER BY` — nếu `unique_key` ≠ `ORDER BY`, data deduplication sẽ không hoạt động đúng.

**Đề xuất:**
```sql
{{ config(
    engine='ReplacingMergeTree',
    unique_key='(province, datetime_hour)',
    order_by='(province, datetime_hour)',  -- ✅ Nhất quán
    partition_by='toYYYYMM(datetime_hour)'
) }}
```

### 5.3 — `dag_weekly_report.py` query table `mart_air_quality__dashboard` không tồn tại trong dbt

**Vấn đề:**
```python
# dag_weekly_report.py lines 90, 119, 138, 165
FROM mart_air_quality__dashboard  -- Table này không có trong dbt models!
```

Sau khi review các mart models (fct_*, dm_*), không tìm thấy `mart_air_quality__dashboard`. Weekly report DAG có thể đang fail silently hoặc dùng legacy table.

**Đề xuất:** Xác định đây là:
- `dm_air_quality_overview_daily` hoặc `fct_air_quality_summary_hourly`?
- Thêm dbt test để verify table tồn tại trước khi report chạy
- Hoặc tạo alias/view `mart_air_quality__dashboard` → mart thực tế

---

## 🟢 Nhóm 6: Monitoring & Observability (Ưu tiên Thấp)

### 6.1 — `log_dbt_stats()` query bằng raw HTTP — không dùng ClickHouse client

**Vấn đề:**
```python
# dag_transform.py lines 335-362
url = f"http://{clickhouse_host}:{clickhouse_port}/?user={clickhouse_user}&password={clickhouse_password}"
response = requests.get(f"{url}&query={query}", timeout=30)
```

Password được append vào URL query string — visible trong server logs, proxy logs, và Airflow task logs. Nên dùng `clickhouse_connect` client đã có trong project.

**Đề xuất:**
```python
import clickhouse_connect

client = clickhouse_connect.get_client(
    host=clickhouse_host,
    username=clickhouse_user,
    password=clickhouse_password,  # Không leak vào URL
)
result = client.query("SELECT count(*) FROM system.tables WHERE ...")
```

### 6.2 — Thiếu end-to-end data freshness alert

**Vấn đề:** Prometheus/Grafana theo dõi infra metrics (CPU, memory, container health) nhưng không có alert cho data freshness. Nếu ingestion fail silently (API token expire, rate limit), dữ liệu trong mart có thể stale trong giờ mà không ai biết.

**Đề xuất:** Thêm ClickHouse freshness metric:
```sql
-- Grafana alert query
SELECT 
    dateDiff('minute', max(last_ingested_at), now()) as minutes_stale
FROM fct_air_quality_province_level_hourly
HAVING minutes_stale > 90  -- Alert nếu data > 90 phút không update
```

### 6.3 — `dag_smoke_test.py` không được trigger trong pipeline chính

Smoke test DAG tồn tại nhưng không được gọi bởi `dag_ingest_hourly` hay `dag_transform`. Nên integrate vào pipeline:
```
ingest → transform → smoke_test → (alert if fail)
```

---

## 🟢 Nhóm 7: Khả năng mở rộng & Production Readiness (Ưu tiên Thấp / Tương lai)

### 7.1 — OpenMetadata stack tiêu tốn ~5GB RAM

```yaml
# docker-compose.yml
elasticsearch: mem_limit: 2g
openmetadata: mem_limit: 2g
execute-migrate-all: (no limit)
```

Tổng: ClickHouse(6g) + Airflow(9g) + OpenMetadata(4g+) + monitoring(1g+) = ~20GB RAM requirement trên single host. Không phù hợp môi trường dev với máy <16GB.

**Đề xuất:**
- Tạo `docker-compose.minimal.yml` không có OpenMetadata + Elasticsearch
- Document minimum requirements rõ ràng trong README
- Thêm `profiles` cho Docker Compose để optional services

### 7.2 — `airflow-init` không chạy DB migration

```yaml
# docker-compose.yml line 393
- AIRFLOW_SKIP_DB_MIGRATE=true  # ❌ Bỏ qua migration!
```

`airflow db migrate` không được chạy khi update Airflow version — schema DB có thể incompatible.

**Đề xuất:**
```yaml
command: >
  bash -c "airflow db migrate && 
           mkdir -p /opt/airflow/logs ..."
# Và bỏ AIRFLOW_SKIP_DB_MIGRATE
```

### 7.3 — Thiếu CI/CD pipeline cho dbt model changes

Không tìm thấy GitHub Actions workflow tự động chạy `dbt compile` + `dbt test --select state:modified` khi có PR merge. Risk: broken SQL deploy thẳng lên production.

**Đề xuất:** Thêm `.github/workflows/dbt-ci.yml`:
```yaml
on: [pull_request]
jobs:
  dbt-check:
    steps:
      - run: dbt compile --target ci
      - run: dbt test --select state:modified+ --target ci
```

---

## 📋 Tổng hợp ưu tiên

| # | Cải tiến | Impact | Effort | Ưu tiên |
|---|----------|--------|--------|---------|
| 2.1 | Sửa `schedule=None` → `@hourly` | 🔴 Critical | Thấp | **Ngay lập tức** |
| 1.2 | Thêm Fernet key cho Airflow | 🔴 Security | Thấp | **Ngay lập tức** |
| 5.3 | Sửa query `mart_air_quality__dashboard` | 🔴 Bug | Trung bình | **Sprint này** |
| 5.2 | Sửa `order_by` ≠ `unique_key` | 🔴 Data bug | Thấp | **Sprint này** |
| 1.1 | Xóa default passwords | 🟠 Security | Thấp | **Sprint này** |
| 2.3 | Sửa timezone Vietnam | 🟠 Logic bug | Thấp | **Sprint này** |
| 2.5 | Fix `records_ingested=0` | 🟡 Observability | Trung bình | Sprint sau |
| 2.2 | Cache `dbt deps` | 🟡 Performance | Trung bình | Sprint sau |
| 3.3 | Seed province classification | 🟡 Maintainability | Trung bình | Sprint sau |
| 4.1 | Persist ChromaDB volume | 🟡 UX | Thấp | Sprint sau |
| 6.1 | Dùng ClickHouse client thay HTTP | 🟡 Security | Thấp | Sprint sau |
| 6.2 | Data freshness alert | 🟢 Monitoring | Trung bình | Backlog |
| 4.4 | Rate limiting `/ask` | 🟢 Security | Thấp | Backlog |
| 7.1 | Minimal docker-compose profile | 🟢 DX | Cao | Backlog |
| 7.3 | CI/CD cho dbt | 🟢 DevOps | Cao | Backlog |

---

## 🎯 Quick Wins (có thể làm ngay trong 1 buổi)

1. **`schedule=None` → `schedule='0 * * * *'`** trong `dag_ingest_hourly.py` — 1 dòng, impact critical
2. **Thêm `execution_timeout`** vào `default_args` tất cả DAGs — 1 dòng
3. **Sửa `order_by` trong `fct_air_quality_province_level_hourly`** — ngăn data dedup bug
4. **Generate Fernet key và thêm vào `.env`** — 1 command
5. **Thêm `AIRFLOW__CORE__FERNET_KEY` vào `.env.example`** với placeholder rõ ràng
