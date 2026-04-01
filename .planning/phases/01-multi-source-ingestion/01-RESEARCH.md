# Phase 1 Research — Multi-Source Ingestion

**Phase:** 01-multi-source-ingestion
**Research Date:** 2026-04-01
**Status:** Research complete — ready for planning

---

## Executive Summary

Three candidate sources were investigated: EPA AirNow, Sensors.Community, and MONRE Vietnam. **EPA AirNow has no Vietnam coverage** — it is a US/Canada-only network. This is a critical finding that must be resolved before Plan 1.1 can proceed. **Sensors.Community is fully viable**: open API, no auth, bounding-box filter works for Vietnam, returns PM2.5/PM10 data. **MONRE Vietnam remains unconfirmed** — the 2-week discovery-first policy is the correct approach; no reliable public API was found during research.

---

## AirNow API

### Authentication & Access

- **Authentication method**: API key passed as a query parameter: `&API_KEY=YOUR_KEY`
- **Registration**: Free account at [airnowapi.org](https://docs.airnowapi.org); key issued upon registration
- **Key format**: Alphanumeric string, no expiry documented for free tier
- **No per-request cost** for the basic developer tier
- **Source**: [AirNow API documentation portal](https://docs.airnowapi.org)

### Endpoints & Data Format

AirNow API is a REST API returning both **JSON and XML** (format selected via `?format=json` or `?format=xml`).

| Endpoint | Purpose | Parameters |
|---|---|---|
| `/1.0/aq/observation` | Current AQI observations | `?zipCode=` or `?lat=` & `?lon=` & `?distance=` |
| `/1.0/forecast/observation` | Forecast + current observations | Same geo params |
| `/1.0/aq/forecast` | AQI forecast | `?zipCode=` or `?lat=` & `?lon=` |
| `/1.0/forecastsByCity` | Forecast by city name | `?city=` |

**Example JSON response shape** (`/aq/observation`):
```json
[
  {
    "DateObserved": "2026-04-01 ",
    "HourObserved": 14,
    "LocalTimeZone": "PST",
    "ReportingArea": "Los Angeles",
    "StateCode": "CA",
    "Latitude": 34.066,
    "Longitude": -118.226,
    "ParameterName": "PM2.5",
    "AQI": 62,
    "Category": { "Number": 2, "Name": "Moderate" }
  }
]
```

Key fields per observation:
- `Latitude`, `Longitude` — station coordinates
- `ReportingArea` — city/area name
- `ParameterName` — pollutant (`PM2.5`, `PM10`, `O3`, `CO`, `NO2`, `SO2`)
- `AQI` — EPA AQI value for that pollutant
- `Category` — health category (1=Good through 6=Hazardous)
- `DateObserved`, `HourObserved`, `LocalTimeZone`

### Vietnam Coverage

> **⚠️ CRITICAL FINDING: AirNow has NO Vietnam coverage.**

AirNow's monitoring station network covers the **United States, Canada, and Mexico only**. Vietnam is not part of the AirNow Gateway network. The AirNow International program shares best practices with other countries but does not integrate their data into the API.

As of 2025, there have been no public announcements about adding Vietnam or broader Southeast Asian coverage to AirNow.

**Impact on Phase 1**: Plan 1.1 (AirNow client) cannot return Vietnam data with the standard AirNow API. Possible paths forward:
1. **AirNow Gateway alternative**: The AirNow Gateway platform (used by US embassies and international partners) may have some non-US stations — requires investigation
2. **AirNow ↔ MONRE feed sharing**: US embassies in Vietnam may feed MONRE data to AirNow Gateway — potentially accessible via `lat=`/`lon=` with Vietnam coordinates
3. **Reclassify AirNow**: Treat AirNow as a US/Canada reference source for AQI methodology, not a Vietnam data source
4. **Replace with different source**: Choose an alternative with confirmed Vietnam coverage (e.g., OpenWeather Air Pollution API, IQAir)

**Recommendation for planning**: Plan 1.1 needs a clarification decision before implementation. The planner must choose one of the paths above.

**Source**: [AirNow API documentation](https://docs.airnowapi.org)

### Rate Limits & Throttling

- AirNow documentation does not publish explicit rate limits for the free developer tier
- A conservative limit of **1 request/second** is commonly used by integrators (matching the roadmap mention of "1 req/sec")
- The existing `TokenBucketRateLimiter` should be configured at `rate_per_second=0.8` with `burst_size=2` as a conservative default
- Retry logic (429 backoff) is already implemented in `api_client.py` via urllib3 `Retry` + `TokenBucketRateLimiter.record_response()`

### Historical Data

- **Lookback**: Approximately **1 day** for observations; forecasts available for ~3–5 days ahead
- AirNow does **not** provide arbitrary historical backfill (no `start_date`/`end_date` parameters)
- Historical data is **not** available via the public API — this conflicts with Decision **D-06** (90-day backfill for AirNow)
- **Implication**: AirNow historical backfill must be scoped out or implemented via a third-party archive (e.g., OpenAQ historical AirNow data, if it exists)
- The `dag_ingest_historical` modification for `source=airnow` may not be viable without an archival source

### Integration Notes

- **Auth pattern differs from existing clients**: AQICN uses token in query param `?token=`, OpenAQ uses `X-API-KEY` header. AirNow also uses query param `&API_KEY=`. The existing `APIClient` supports `auth_header_name=None` (for AQICN) but always adds an auth header if a token is set. Best approach: pass `auth_header_name=None` and manually inject `API_KEY` into params in a wrapper factory.
- **JSON response is a list** (array), not an object with `results` — `PaginatedAPIClient.fetch_all()` uses `response.get("results", [])` and will need adjustment for AirNow's list-of-objects format
- **Reconsider Plan 1.1**: The Vietnam coverage gap makes AirNow's value to this project uncertain. Planners should revisit the decision before code is written.

---

## Sensors.Community API

### Authentication & Access

- **No authentication required** — the API is fully public
- **Base URL**: `https://api.sensor.community/v1/feeds/`
- **Source**: Community-driven project (formerly Luftdaten/PurpleAir community fork)
- **Governance**: Open source, maintained by OpenKnowledgeContainer / OpenSenseMap ecosystem

### Endpoints & Data Format

The primary endpoint is **`/v1/feeds/`** which returns a JSON array of sensor station records.

**Query parameters for filtering:**

| Parameter | Type | Description |
|---|---|---|
| `lat` | float | Center latitude for bounding box |
| `latDelta` | float | Latitude radius/spread |
| `lng` | float | Center longitude for bounding box |
| `lngDelta` | float | Longitude radius/spread |
| `node` | string | Filter by node/sensor ID |
| `indoor` | boolean | Filter indoor vs outdoor sensors |
| `sensor_type` | string | Filter by sensor model (e.g., `sds011`, `pms5003`) |

**Vietnam bounding box filter** (using project bbox: lat 8.4°N–23.4°N, lon 102.1°E–109.5°E):
```
https://api.sensor.community/v1/feeds/?lat=16.0&latDelta=7.5&lng=105.0&lngDelta=7.0
```
Center point: ~16°N, 105°E. Covers the full Vietnam rectangle.

**Optional sensor type filter** (PM sensors only):
```
https://api.sensor.community/v1/feeds/?lat=16.0&latDelta=7.5&lng=105.0&lngDelta=7.0&sensor_type=sds011
```

**JSON response shape** (per station):
```json
{
  "id": 12345,
  "sensor": {
    "id": 67890,
    "sensor_type": { "name": "SDS011" },
    "pin": "1"
  },
  "location": {
    "latitude": 10.8231,
    "longitude": 106.6297,
    "country": "VN"
  },
  "data": [
    {
      "sensordatavalues": [
        { "value_type": "P1", "value": 45.2 },   // PM10 ( µg/m³)
        { "value_type": "P2", "value": 28.7 }    // PM2.5 ( µg/m³)
      ],
      "timestamp": "2026-04-01T10:30:00"
    }
  ]
}
```

Key field mappings:
- `P1` → PM10 (µg/m³)
- `P2` → PM2.5 (µg/m³)
- `temperature` → air temperature (°C), if available
- `humidity` → relative humidity (%), if available

### Vietnam Coverage (with Bounding Box Filter)

The bounding box approach works — Vietnam falls cleanly within:
- Center: ~16°N, 105°E
- Lat span: 8.4°N to 23.4°N → `latDelta ≈ 7.5`
- Lon span: 102.1°E to 109.5°E → `lngDelta ≈ 7.0`

**Important caveats:**
- Station density in Vietnam is **unknown** — the Sensors.Community network is volunteer/community-driven; rural Vietnam may have few sensors
- Stations outside the Vietnam bbox but within the API response should receive `quality_flag = 'outlier'` per Decision **D-14**
- `indoor` parameter should be set to `false` to exclude non-environmental sensors
- Country code field (`location.country = "VN"`) may provide a secondary filter if the API returns it

**No built-in country filter** — the API requires lat/lng bounding box; there is no `?country=VN` parameter.

### Data Quality Characteristics

Sensor.Community sensors are **DIY community devices** — quality is variable:

| Factor | Detail |
|---|---|
| **Sensor accuracy** | SDS011: ±10% + 10µg/m³ for PM2.5; acceptable for trend monitoring, not reference-grade |
| **Known biases** | SDS011 tends to overestimate PM2.5 in high-humidity environments (Vietnam climate) |
| **Uptime** | Community sensors can go offline; no guaranteed uptime |
| **Calibration** | No factory calibration; self-calibration algorithms applied in firmware |
| **GPS accuracy** | Location accuracy varies; sensors may be at imprecise coordinates |
| **Sensor age** | Old sensors may drift; degradation over 1–2 years |

**PM2.5 range validation** (per Decisions **D-11**, **D-12**):
- `quality_flag = 'implausible'`: PM2.5 outside 0–500 µg/m³ (sensor malfunction or extreme event)
- `quality_flag = 'outlier'`: Valid range but statistically anomalous (>3σ from rolling mean) or geographic filter triggered
- `quality_flag = 'valid'`: Normal range (0–500 µg/m³), within statistical bounds
- All data is inserted regardless of quality flag (Decision **D-11**)

**Humidity correction**: Humidity >70% can cause false elevated PM readings from SDS011. The `int_data_quality` dbt model (Phase 2) should consider humidity correction for Sensors.Community data specifically.

### Historical Data

- The Sensors.Community API returns **current/latest readings** per sensor — no historical endpoint exists
- To get historical data: the sensor must have been polled previously and data stored
- **Implication**: Historical backfill (Decision **D-07**, 90-day) is **not possible** via the Sensors.Community API directly
- **Workaround**: If OpenAQ ingests Sensors.Community data (which it does via the Luftdaten adapter), OpenAQ's historical data may serve as a partial archive — but OpenAQ itself is being decommissioned in Plan 1.4
- **Realistic scope**: Only current/real-time ingestion is viable; backfill is limited to whatever the polling run captured

### Integration Notes

- **No auth**: Directly instantiate `APIClient` with no token, no headers
- **Rate limiting**: Not officially documented; community API has no published limits. Use `rate_per_second=1.0` (1 req/s) as a courtesy maximum, with `burst_size=5` for safety
- **Polling interval**: Plan 1.2 specifies `*/10 * * * *` (every 10 minutes). The Sensors.Community network updates approximately every 5–10 minutes. A 10-minute poll should capture most readings without over-polling.
- **No pagination documented**: The API returns all matching stations in one response. For Vietnam bbox, the result set should be small (hundreds, not thousands). No cursor/page mechanism needed.
- **Vietnam bbox check in code**: The bounding box filter is on the API side, but the client code should also validate `latitude`/`longitude` from the response against the Vietnam bbox (lat 8.4°N–23.4°N, lon 102.1°E–109.5°E) as a secondary filter, since API-side bounding boxes can return edge-case stations outside the box.

**Recommended factory function** (for `api_client.py`):
```python
def create_sensorscm_client() -> APIClient:
    from .rate_limiter import TokenBucketRateLimiter
    limiter = TokenBucketRateLimiter(rate_per_second=1.0, burst_size=5)
    return APIClient(
        base_url="https://api.sensor.community",
        token=None,
        timeout=30,
        max_retries=5,
        rate_limiter=limiter,
        auth_header_name=None
    )
```

**Source**: [Sensor.Community API v1](https://api.sensor.community/v1)

---

## MONRE Vietnam

### Access Methods Investigated

Three access methods were investigated:

| Method | Status | Notes |
|---|---|---|
| **Official API** (vea.gov.vn) | **Not confirmed** — no public API documentation found | Requires direct contact with MONRE |
| **Web portal scraping** (vea.gov.vn, moitruongvn.com) | Viable but fragile | JS-rendered pages need Playwright; terms of service may prohibit scraping |
| **data.gov.vn** (Vietnam Open Data Portal) | **DNS resolution failed** during research | Worth revisiting; may have air quality datasets |
| **Direct data agreement** | Required for official/sustained access | MONRE may require a formal MOU |
| **Third-party aggregation** | AQICN, IQAir already do this | MONRE data reaches these platforms |

**Government open data mandate**: Vietnam's Law on Environmental Protection (2020) includes an open data provision. MONRE should provide environmental data publicly, but the implementation of programmatic access is unclear.

### Recommended Approach

Per the Phase 1 decisions (**D-16** through **D-21**): **2-week discovery-first, non-blocking to Phase 1**.

Discovery steps should be executed in order:

1. **Week 1, Day 1–2**: Attempt HTTP requests against known MONRE/CEM endpoints:
   - `http://vea.gov.vn` and subdomains
   - `http://cems.gov.vn` (Center for Environmental Monitoring)
   - `http://airquality.gov.vn` (if exists)
   - Check HTTP response codes, look for JSON APIs

2. **Week 1, Day 3–4**: Use Playwright to explore the JS-rendered portal:
   - Navigate the air quality dashboard at vea.gov.vn
   - Inspect network requests (XHR/Fetch) in browser DevTools to find data endpoints
   - Capture any undocumented API URLs

3. **Week 1, Day 5**: Check data.gov.vn:
   - Search for "air quality", "PM2.5", "AQI", "môi trường" datasets
   - Look for CSV/JSON downloads

4. **Week 2, Day 1–3**: Formal contact:
   - Email MONRE's Department of Science, Technology and International Cooperation
   - Request API access or data sharing agreement
   - Document the response (or lack thereof) for the archive decision

5. **Week 2, Day 4–5**: Evaluate findings:
   - If API or scraping method confirmed → implement `dag_monre_ingest`
   - If not confirmed → archive Plan 1.3, Phase 1 ships complete with 3 sources

### MONRE Data Format (If Found)

Based on similar government monitoring systems in Southeast Asia:

| Field | Type | Example |
|---|---|---|
| `station_id` | string | `HN_001` |
| `station_name` | string | `Hà Nội - Tự nhiên` |
| `timestamp` | ISO 8601 | `2026-04-01T10:00:00+07:00` |
| `pm25` | float µg/m³ | `68.5` |
| `pm10` | float µg/m³ | `112.0` |
| `co` | float mg/m³ | `0.8` |
| `no2` | float µg/m³ | `45.0` |
| `o3` | float µg/m³ | `32.0` |
| `so2` | float µg/m³ | `12.0` |
| `aqi` | integer | `156` |
| `latitude` | float | `21.0285` |
| `longitude` | float | `105.8542` |

**Vietnam AQI calculation** uses the Vietnam NAQMP (National Air Quality Monitoring Programme) scale, which may differ slightly from EPA AQI. Phase 2 dbt models should handle this.

### Discovery Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| No public API found | **High** | Archive Plan 1.3 per D-19; Phase 1 ships with 3 sources |
| Portal uses JS rendering (can't scrape with requests) | **High** | Use Playwright (add to `requirements.txt` if not present) |
| Data format unstable / changes without notice | **Medium** | Store `raw_payload` JSON; implement schema migration in Phase 2 |
| Rate limits or IP blocking | **Medium** | Respect robots.txt; add retry with backoff |
| Data quality issues (gaps, invalid values) | **Medium** | Apply same quality_flag logic as Sensors.Community |
| Data agreement required for bulk access | **Medium** | Document in discovery report; continue without MONRE |

### MONRE Grafana Panel

Per Decision **D-20**: Grafana MONRE status panel shows:
- `'active'` — data flowing from MONRE source
- `'discovering'` — 2-week window open, discovery in progress
- `'archived'` — timebox expired, no viable access found

Status should be driven by `ingestion.control.source = 'monre'` (or `'discovering'` as a placeholder row inserted by `dag_metadata_update` during the window).

---

## Existing Codebase Alignment

### How to Extend `api_client.py`

The existing `APIClient` base class supports all required patterns:

**AirNow** (requires custom factory — token in query param, not header):
```python
# Pattern: auth_header_name=None, inject API_KEY into params manually
def create_airnow_client(api_key: str) -> APIClient:
    from .rate_limiter import TokenBucketRateLimiter
    limiter = TokenBucketRateLimiter(rate_per_second=0.8, burst_size=2)
    client = APIClient(
        base_url="https://www.airnowapi.org/1.0",
        token=None,
        timeout=30,
        max_retries=5,
        rate_limiter=limiter,
        auth_header_name=None  # Key goes in params, not header
    )
    client.api_key = api_key  # Store for manual injection
    return client
```

**Sensors.Community** (no auth):
```python
def create_sensorscm_client() -> APIClient:
    from .rate_limiter import TokenBucketRateLimiter
    limiter = TokenBucketRateLimiter(rate_per_second=1.0, burst_size=5)
    return APIClient(
        base_url="https://api.sensor.community",
        token=None,
        timeout=30,
        max_retries=5,
        rate_limiter=limiter,
        auth_header_name=None
    )
```

**Important caveat for AirNow**: The `APIClient.request()` method builds the URL with `urlencode(params)`. For AirNow, `API_KEY` must be in `params`, but the existing code injects the token as a header. Two options:
1. Override `request()` in a subclass `AirNowAPIClient(APIClient)`
2. Pass `API_KEY` as part of the `params` dict in each call (simple, no subclass needed)

The `PaginatedAPIClient` also needs adjustment for AirNow since its response is a **list** (not `{"results": [...]}`). The `fetch_all()` method uses `response.get("results", [])` which will return `[]` for AirNow's list-of-objects format. A simple override or a response parser parameter is needed.

### ClickHouse Schema Recommendations

Per Decisions **D-01** through **D-05**:

**AirNow tables** (`raw_airnow_*`):

```sql
-- Measurements
CREATE TABLE IF NOT EXISTS raw_airnow_measurements
(
    source              LowCardinality(String) DEFAULT 'airnow',
    ingest_time         DateTime DEFAULT now(),
    ingest_batch_id     String,
    ingest_date         Date MATERIALIZED toDate(ingest_time),

    station_id          String,                    -- Derived from lat/lon hash or reporting area
    reporting_area      String,
    state_code          String,
    latitude            Float64,
    longitude           Float64,

    timestamp_local     Nullable(String),          -- DateObserved + HourObserved + LocalTimeZone
    timestamp_utc       Nullable(DateTime),         -- Converted UTC

    parameter_name      LowCardinality(String),     -- PM2.5, PM10, O3, NO2, SO2, CO
    aqi_reported        UInt16,                     -- EPA AQI as reported (D-23)
    category_number     UInt8,                      -- 1=Good through 6=Hazardous
    category_name       String,

    raw_payload         String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(ingest_date)
ORDER BY (station_id, timestamp_utc, parameter_name)
SETTINGS index_granularity = 8192;

-- Sites/stations metadata
CREATE TABLE IF NOT EXISTS raw_airnow_sites
(
    source              LowCardinality(String) DEFAULT 'airnow',
    ingest_time         DateTime DEFAULT now(),
    ingest_batch_id     String,
    ingest_date         Date MATERIALIZED toDate(ingest_time),

    station_id          String,
    reporting_area      String,
    state_code          String,
    latitude            Float64,
    longitude           Float64,

    raw_payload         String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(ingest_date)
ORDER BY (station_id)
SETTINGS index_granularity = 8192;
```

**Sensors.Community tables** (`raw_sensorscm_*`):

```sql
-- Measurements
CREATE TABLE IF NOT EXISTS raw_sensorscm_measurements
(
    source              LowCardinality(String) DEFAULT 'sensorscm',
    ingest_time         DateTime DEFAULT now(),
    ingest_batch_id     String,
    ingest_date         Date MATERIALIZED toDate(ingest_time),

    sensor_id           UInt32,
    station_id          UInt32,                    -- id from API (reuse as station_id)
    latitude            Float64,
    longitude           Float64,

    timestamp_utc       Nullable(DateTime),
    parameter           LowCardinality(String),    -- P1 (PM10), P2 (PM2.5), temperature, humidity
    value               Float32,
    unit                String DEFAULT 'µg/m³',

    sensor_type        LowCardinality(String),    -- SDS011, PMS5003, etc.
    quality_flag        LowCardinality(String),    -- valid, outlier, implausible (D-12, D-14)

    raw_payload         String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(ingest_date)
ORDER BY (station_id, timestamp_utc, parameter)
SETTINGS index_granularity = 8192;

-- Sensors/sensor metadata
CREATE TABLE IF NOT EXISTS raw_sensorscm_sensors
(
    source              LowCardinality(String) DEFAULT 'sensorscm',
    ingest_time         DateTime DEFAULT now(),
    ingest_batch_id     String,
    ingest_date         Date MATERIALIZED toDate(ingest_time),

    sensor_id           UInt32,
    station_id          UInt32,
    latitude            Float64,
    longitude           Float64,
    sensor_type         LowCardinality(String),
    is_indoor           Bool DEFAULT false,

    raw_payload         String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(ingest_date)
ORDER BY (sensor_id)
SETTINGS index_granularity = 8192;
```

**MONRE table** (schema TBD after discovery — design for flexibility):
```sql
-- MONRE measurements (tentative schema — confirm after discovery)
CREATE TABLE IF NOT EXISTS raw_monre_measurements
(
    source              LowCardinality(String) DEFAULT 'monre',
    ingest_time         DateTime DEFAULT now(),
    ingest_batch_id     String,
    ingest_date         Date MATERIALIZED toDate(ingest_time),

    station_id          String,
    station_name        Nullable(String),
    latitude            Float64,
    longitude           Float64,

    timestamp_utc       Nullable(DateTime),
    parameter           LowCardinality(String),    -- pm25, pm10, co, no2, o3, so2, aqi
    value               Float32,
    unit                String,
    aqi_reported        Nullable(UInt16),
    quality_flag        LowCardinality(String) DEFAULT 'valid',

    raw_payload         String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(ingest_date)
ORDER BY (station_id, timestamp_utc, parameter)
SETTINGS index_granularity = 8192;
```

### Airflow Integration Patterns

**`dag_ingest_hourly` modifications** (Plan 1.5):
- AirNow + Sensors.Community tasks run **in parallel** with existing AQICN tasks (Decision **D-56**)
- New task pattern (per existing `run_aqicn_measurements_ingestion`):
```python
@task
def run_airnow_measurements_ingestion():
    env = os.environ.copy()
    env.update(get_job_env_vars())
    cmd = f"cd {PYTHON_PATH} && python jobs/airnow/ingest_measurements.py --mode incremental"
    result = subprocess.run(cmd, shell=True, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Command failed: {cmd}")
```

**`dag_sensorscm_poll`** (new, Plan 1.2):
- Schedule: `*/10 * * * *` (every 10 minutes)
- Runs `python jobs/sensorscm/ingest_measurements.py --mode incremental`
- Does NOT need `check_clickhouse_connection` (can reuse shared task or rely on timeout)
- Updates `ingestion.control` as final task

**Rate limiter sharing**: Per Decision **D-25**, one `TokenBucketRateLimiter` per API key, shared across tasks. The existing module-level singleton pattern in `rate_limiter.py` (via `create_*_limiter()` factory functions) handles this — each factory creates/returns a shared instance.

**AirNow DAG placement**: Per CONTEXT.md, AirNow is integrated into `dag_ingest_hourly` (hourly), not a separate DAG.

**`dag_ingest_historical` modification**: Adding `source` parameter to allow `source=airnow` or `source=sensorscm`. The DAG currently uses `--days-back` CLI arg; adding `--source` allows per-source backfill. Historical backfill for AirNow may not be viable (see AirNow § Historical Data above).

---

## Validation Architecture

### AirNow Validation

| Check | Pass Condition | Fail Action |
|---|---|---|
| **Non-empty response** | Response array has at least 1 record | Log warning; continue (may be no stations in bbox) |
| **Required fields present** | `Latitude`, `Longitude`, `ParameterName`, `AQI`, `DateObserved` all present | Skip record; log to `raw_payload` |
| **AQI range** | 0–500 (EPA AQI scale) | Flag as `implausible`; insert anyway |
| **Timestamp valid** | ISO date parseable | Skip record |
| **Vietnam bbox** | lat 8.4°N–23.4°N, lon 102.1°E–109.5°E | Skip record (should not occur with API bbox filter) |
| **No HTTP errors** | Response status 200 | Retry via existing tenacity backoff |
| **Zero duplicates** | ClickHouse ReplacingMergeTree dedupes on `(station_id, timestamp_utc, parameter)` | N/A — server-side dedup |

**Acceptance criteria**: `ingestion.control.airnow` updated with `records_ingested > 0` within 5 minutes of task start for at least 3 consecutive runs.

**Critical note**: If AirNow returns 0 records for Vietnam bbox, Plan 1.1 success criteria are not met. Planners must decide: continue with AirNow despite zero Vietnam data, or replace with an alternative source.

### Sensors.Community Validation

| Check | Pass Condition | Fail Action |
|---|---|---|
| **Response non-empty** | At least 1 station in Vietnam bbox | Warn — may indicate API outage or bbox error |
| **Valid JSON** | Parses without error | Retry |
| **Required fields** | `location.latitude`, `location.longitude`, `data[].sensordatavalues` | Skip record |
| **PM value range** | 0–500 µg/m³ | `quality_flag = 'implausible'`; insert with flag |
| **Vietnam bbox** | lat 8.4°N–23.4°N, lon 102.1°E–109.5°E (secondary check) | `quality_flag = 'outlier'`; insert with flag |
| **Statistical outlier** | Within 3σ of rolling 24h mean for that sensor | `quality_flag = 'outlier'`; insert with flag |
| **Stale sensor** | Last reading >1 hour old | Log warning; skip or flag |

**Implausible PM2.5 threshold**: >500 µg/m³ or <0 µg/m³. PM10 also capped at 500 µg/m³.

**Outlier detection**: For Phase 1, a simple 3σ check using a ClickHouse materialized view (or subquery) on the sensor's rolling 24-hour window is sufficient. Complex anomaly detection (Isolation Forest, etc.) is out of scope for Phase 1.

**Acceptance criteria**: `ingestion.control.sensorscm` updated with `records_ingested > 0` within 2 minutes for at least 5 consecutive runs; `quality_flag` distribution logged (expect <5% `implausible`, <10% `outlier`).

### MONRE Validation

Not applicable until access method is confirmed (2-week discovery window). Post-discovery, validation will depend on confirmed schema.

---

## Key Findings & Recommendations

### Critical Issues for Planning

1. **AirNow has NO Vietnam coverage** — this is the most important finding. Plan 1.1 cannot return Vietnam data via the standard AirNow API. Planners MUST resolve this before writing code. Options: investigate AirNow Gateway, reclassify AirNow as a US reference source, or replace with an alternative API (OpenWeather, IQAir).

2. **Sensors.Community has no historical backfill** — Decision D-07 (90-day backfill) is not achievable via the Sensors.Community API, which only returns current/latest readings. Real-time ingestion is viable; historical is not.

3. **AirNow has no historical backfill** — Same issue as above; AirNow API provides ~1 day of lookback. Decision D-06 (90-day AirNow backfill) may need to be revised.

4. **MONRE access is unconfirmed** — The 2-week discovery policy is correct. Research found no public API documentation; Playwright exploration of the JS-rendered portal is the most promising next step.

5. **Sensors.Community station density in Vietnam is unknown** — Network is volunteer-driven. Rural Vietnam may have very few sensors. The bounding box API filter works, but the result set size is uncertain.

### High-Priority Planning Decisions Needed

- **D-AIRNOW-1**: What to do about AirNow's Vietnam coverage gap? (AirNow Gateway? Alternative source? Accept 0 Vietnam records?)
- **D-AIRNOW-2**: Should AirNow historical backfill (Decision D-06) be removed from Phase 1 scope given API limitations?
- **D-SENSORSC-1**: Should Decision D-07 (90-day Sensors.Community backfill) be removed given API limitations?
- **D-MONRE-1**: Is Playwright available in the container? If not, add it to `requirements.txt` for MONRE discovery.

### High-Value Findings

- **Sensors.Community is the most straightforward new source**: No auth, public API, bounding box filter works for Vietnam, returns PM2.5/PM10 directly. Should be the first implementation.
- **AirNow API auth pattern is compatible**: Query param `&API_KEY=` fits the existing `APIClient` design with a minor factory adjustment.
- **ReplacingMergeTree dedup is already proven**: The AQICN `raw_aqicn_forecast` table uses this pattern — replicate exactly for new tables.
- **Vietnam bbox is well-established**: The bbox (lat 8.4°N–23.4°N, lon 102.1°E–109.5°E) is already in Decisions and codebase. Use it as-is for all new sources.

### Sequencing Recommendation

Given the findings above, the recommended implementation order is:
1. **Sensors.Community** (Plan 1.2) — highest confidence, no Vietnam coverage concerns
2. **AirNow** (Plan 1.1) — contingent on resolving the Vietnam coverage decision first
3. **MONRE** (Plan 1.3) — 2-week discovery in parallel with Plans 1.1 and 1.2
4. **Rate limiter + Airflow** (Plan 1.5) — optimize after all sources are ingesting
5. **OpenAQ decommission** (Plan 1.4) — last, after new sources are verified

---

## Sources

- [AirNow API Documentation Portal](https://docs.airnowapi.org)
- [Sensor.Community API v1](https://api.sensor.community/v1)
- [VEAG Portal — MONRE Vietnam](http://vea.gov.vn)
- [Vietnam Open Data Portal — data.gov.vn](http://data.gov.vn)
- [Vietnam Law on Environmental Protection 2020](http://vea.gov.vn) (open data mandate)
- [Sensor.Community Map](https://maps.sensor.community/)
