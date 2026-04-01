# AirNow Alternatives Research

*Research Date: April 2026*
*Prepared for: Vietnam Air Quality Data Platform — Phase 01 Multi-Source Ingestion*

---

## Executive Summary

The EPA AirNow API (airnowapi.org) is US/Canada-exclusive and provides no Vietnam coverage. This research evaluates four primary alternative sources for a second authoritative air quality feed for Vietnam:

| Rank | Source | Vietnam Coverage | Data Quality | Cost | Integration Ease |
|------|--------|-----------------|---------------|------|-----------------|
| 1 | **OpenWeather Air Pollution API** | ✅ Global (any lat/lon) | Moderate (model + station blend) | Free tier excellent (60 req/min, 1M/month) | Easy (simple REST, lat/lon based) |
| 2 | **WAQI / World Air Quality Index** | ✅ Strong (Hanoi, HCMC, Da Nang + others) | High (11,000+ real stations) | **Free** | Moderate (station-based, not lat/lon) |
| 3 | **IQAir AirVisual API** | ✅ Moderate–High (major cities) | High (research-grade monitors) | Free tier limited; paid from ~$49/mo | Moderate (city/station endpoints) |
| 4 | **US Embassy / AirNow International** | ⚠️ Single point (Hanoi only) | High (EPA-grade reference monitor) | Free | Moderate (web scraping or AirNow index page) |

**Top recommendation: OpenWeather + WAQI as a dual-source complement to the existing AQICN pipeline.** Both have free tiers, broad Vietnam coverage, and straightforward APIs. WAQI has more real monitoring stations; OpenWeather offers forecast and historical data natively.

---

## 1. US Embassy / AirNow Gateway

### Coverage

- The **U.S. Embassy in Hanoi** operates a real-time EPA-grade PM2.5 monitoring station on its compound.
- Data is published on the Embassy website (`vn.usembassy.gov`) and mirrored on **AirNow International** (`airnow.gov`).
- **Single location only** — Hanoi US Embassy compound. No coverage for HCMC, Da Nang, or other Vietnamese cities.
- The Embassy data is widely regarded in Vietnam as the most trustworthy independent reference, often running higher than MONRE's official readings.

### API Details

The Embassy does **not** expose a public machine-readable API. Data access options are:

1. **Web scraping** — `https://vn.usembassy.gov/our-relationship/air-quality-monitor-hanoi/` displays hourly PM2.5 readings. Page structure may change without notice.
2. **AirNow International dashboard** — `https://www.airnow.gov/index.php?id=608` — search for "Hanoi, US Embassy" for a live chart with historical data. No formal API access.
3. **AirNow Gateway XML feed** — `https://www.airnow.gov/index.cfm?action=airnow.gateway_summary` — This is the US domestic AirNow data feed; Hanoi is listed under AirNow International, but access may require registration and the feed is primarily US-focused. Unclear if Hanoi data is included programmatically.

There is **no documented REST API** for Embassy/consular AQ data.

### Feasibility for Vietnam

- **Not viable as a primary data source** due to single-location coverage.
- **Viable as a supplementary high-quality reference point** for Hanoi validation/correlation.
- Web scraping introduces fragility (page structure changes break pipelines) and should be considered a last resort.
- The Embassy reading and MONRE readings can be used to cross-validate AQI calculations.

### Recommendation

**Use as a validation signal only.** Do not build a pipeline dependency on scraped Embassy data. If scraping is attempted, implement HTML parsing with fallback alerting for page structure changes. The single-point coverage is insufficient for city-level or regional AQ analysis.

---

## 2. OpenWeather Air Pollution API

### Vietnam Coverage

- **Global coverage for any lat/lon pair** — Vietnam bounding box (lat 8.4°N–23.4°N, lon 102.1°E–109.5°E) is fully covered.
- Data is a **blend of in-situ stations and satellite/road-model estimates** — not purely observational.
- Resolution depends on data density in the region; Vietnam has reasonable station coverage in major cities (Hanoi, HCMC, Da Nang).
- Vietnam-specific MONRE/government station integration is unclear.

### API Details

Three REST endpoints, all based on lat/lon coordinates:

| Endpoint | Purpose | URL Pattern |
|----------|---------|-------------|
| Current | Real-time pollution at a point | `https://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={key}` |
| Forecast | Hourly forecast up to 4 days | `https://api.openweathermap.org/data/2.5/air_pollution/forecast?lat={lat}&lon={lon}&appid={key}` |
| Historical | Historical data from Nov 27, 2020 | `https://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={unix}&end={unix}&appid={key}` |

**Data fields returned** (per the documentation):
- `co` — Carbon monoxide (μg/m³)
- `no` — Nitrogen monoxide (μg/m³)
- `no2` — Nitrogen dioxide (μg/m³)
- `O3` — Ozone (μg/m³)
- `so2` — Sulfur dioxide (μg/m³)
- `pm2_5` — Fine particulate matter (μg/m³)
- `pm10` — Coarse particulate matter (μg/m³)
- `nh3` — Ammonia (μg/m³)
- `aqi` — Air Quality Index (values 1–5: Good to Very Poor)

**Authentication:** API key via `appid` query parameter.

### Pricing

| Plan | Calls/Minute | Calls/Month | Price |
|------|-------------|-------------|-------|
| **Free** | **60** | **1,000,000** | **Free** |
| Startup | 600 | 10,000,000 | Not displayed (requires quote) |
| Developer | 3,000 | 100,000,000 | Not displayed |
| Professional | 30,000 | 1,000,000,000 | Not displayed |
| Expert | 100,000 | 3,000,000,000 | Not displayed |

The **free tier is extremely generous** for a data engineering project — 1M calls/month at 60 req/min easily supports polling multiple Vietnam city coordinates every 15 minutes.

### Recommendation

**Strongly recommended as the primary AirNow replacement.** Free tier limits are more than sufficient for hourly ingestion across 10–20 Vietnam locations. The combination of current + forecast + historical endpoints covers all three data paths needed by the platform. The only caveat is that data quality is modeled/estimated rather than purely observational — cross-validation against real monitoring stations (WAQI, AQICN) is advisable.

---

## 3. WAQI / World Air Quality Index

### Vietnam Coverage

- **Strong Vietnam coverage** across major cities: Hanoi, Ho Chi Minh City, Da Nang, Hai Phong, Can Tho, and others.
- Network of **11,000+ station-level readings globally**, including both government (MONRE) and community-contributed stations.
- Vietnam is served by a dedicated `/api/vn/` path with Vietnamese language support on the website.
- **aqicn.org and waqi.info are the same project** — the World Air Quality Index Project. The current API token from the project's data platform is used for `api.waqi.info`. AQICN (AQICN.com) referenced in the existing project token may be a legacy domain or partner; verify which token format applies.

### API Details

**Primary JSON API endpoint:**
```
http://api.waqi.info/feed/[location]/?token=[token]
```

`[location]` can be:
- A city name: `/feed/hanoi/?token=...`
- A station name: `/feed/@station-id/?token=...`
- A bounding box: `/feed/geo:{lat1};{lon1};{lat2};{lon2}/?token=...`
- Lat/lon: `/feed/geo:{lat};{lon}/?token=...`
- IP address geolocation: `/feed/geo:{ip}/?token=...`

**Additional APIs:**
- **Map Tile API** — for embedding AQI overlays on map visualizations (useful for Superset/Grafana integration).
- **Widget API** — for embedding real-time AQI widgets.

**Data fields available per station:**
- AQI (EPA-standard scale)
- PM2.5, PM10, NO2, CO, SO2, Ozone concentrations
- Station name and coordinates
- Current weather conditions (temperature, humidity, pressure, wind)
- Air quality forecasts (3–8 days ahead)

**Authentication:** Token registered at `https://aqicn.org/data-platform/token/` (free registration).

### Pricing

**Free.** The WAQI API is provided free of charge, including for commercial use (subject to Terms of Service). Rate limit is approximately **1,000 requests/second** with UNEP support backing — more than adequate for any reasonable ingestion schedule.

> ⚠️ **Note on tokens:** The existing project notes an "IQAir/AQICN token already exists." WAQI (aqicn.org/waqi.info) and IQAir (iqair.com) are separate organizations. Verify whether the existing token is a WAQI token or an IQAir token, as the APIs are different.

### Recommendation

**Highly recommended as a second source alongside OpenWeather.** The station-based coverage is more geographically granular than OpenWeather's model-based approach, and the free tier is unconditionally free (no billing card required). The bounding-box geo query (`/feed/geo:{lat1};{lon1};{lat2};{lon2}/`) can return all stations within the Vietnam bounding box in a single call, making it easy to enumerate all stations each run.

---

## 4. IQAir AirVisual (vs. AQICN)

### Coverage

- IQAir AirVisual maintains **government, research-grade, and community stations** across Vietnam.
- Coverage includes at minimum: Hanoi, Ho Chi Minh City, Da Nang, Hai Phong, Can Tho.
- IQAir is a **separate organization from WAQI/aqicn.org** — they are distinct data sources with different station networks and API platforms.
- Some stations may appear in both WAQI and IQAir networks, but the data feeds are independent.

### API Details

IQAir's API is accessed via `api.aqi.in` (redirected from `api.aqi.in`):

**Key endpoints:**

| Endpoint | Description |
|----------|-------------|
| `GET /v2/stations?country=VN` | List all Vietnam stations |
| `GET /v2/stations/{station_id}` | Individual station live readings |
| `GET /v2/cities/{city}` | City-level aggregated AQI (e.g., `/v2/cities/hanoi`) |
| `GET /v2/modules/{module_id}` | Specific monitor module data |
| `GET /v2/history/{city}` | Historical data (paid plans) |

**Data fields:** AQI, PM2.5, PM10, CO, NO2, O3, SO2 (concentrations + AQI values).

### Pricing

| Plan | Approx. Price | API Calls/Month | Key Limits |
|------|--------------|-----------------|------------|
| **Free (Starter)** | **Free** | **~2,000** | City-level only; no history; no station-level |
| Bronze | ~$49/mo | ~10,000 | Station-level data, limited history |
| Silver | ~$149/mo | ~50,000 | Full station-level, 1-year history |
| Gold | ~$399/mo | ~200,000 | Extended history, bulk queries |
| Enterprise | Custom | Unlimited | Custom SLA, dedicated support |

**Free tier limitation:** Station-level data (individual monitors) is generally **not available on the free tier** — only city-level aggregated AQI. This significantly reduces the value of the free tier for a multi-station pipeline.

### Recommendation

**Lower priority for initial integration.** The free tier is too limiting for station-level ingestion. If the platform budget allows, the Bronze or Silver tier unlocks genuine value. However, given that WAQI provides a comparable (or larger) station network **for free**, IQAir should be considered an enhancement layer rather than a primary second source. The existing AQICN token should be verified — if it is an IQAir token (not WAQI), it may be worth retaining IQAir as a third source rather than replacing AQICN.

---

## 5. Other Options

### PurpleAir
- **Primarily a US-centric community sensor network** (120+ countries but heavily US-weighted).
- Vietnam coverage is likely very limited or absent — PurpleAir's map shows sparse coverage in Southeast Asia.
- Offers a JSON API at `https://www.purpleair.com/json` (no authentication required for basic queries).
- **Not recommended for Vietnam** due to insufficient coverage.

### BreezoMeter
- Originally a freemium air quality API. As of 2024–2025, BreezoMeter's API appears to be **redirected/merged into Google Maps Platform** (mapsplatform.google.com/maps-products/#environment-section).
- This suggests BreezoMeter has been acquired/integrated into Google's environmental products and no longer operates as an independent freemium API.
- **Not recommended** — unclear current status and likely commercial-only.

### MONRE (Vietnam Government)
- Vietnam's Ministry of Natural Resources and Environment operates a national air quality monitoring network.
- Real-time data portal: `http://envi.web.vn/` and `http://aqms.nea.gov.vn/`.
- **No public documented API** — data is typically published via a web portal with limited historical access.
- Scraping or partnership/licensing would be required. Low feasibility for an automated pipeline without government agreement.
- **Consider as a future enhancement** if a formal data-sharing agreement can be established.

### Sensor.Community (luftdaten.info)
- Community sensor network with some global coverage.
- **Sparse in Vietnam** — useful as supplementary data but not authoritative.
- Data is accessible via the Sensor.Community API and has been partially integrated already in the existing codebase.

---

## Recommendation Summary

### Primary Replacement: OpenWeather Air Pollution API

**Best overall choice** combining:
- Unlimited (free tier) usage well within project needs
- Global coverage including all of Vietnam via lat/lon
- Three endpoint types: current, forecast (4-day hourly), historical (from Nov 2020)
- Simple REST API with no complex authentication
- PM2.5, PM10, O3, NO2, SO2, CO, NH3 + EPA AQI index

**Integration approach:** Poll 10–20 pre-defined Vietnam city/town coordinates (Hanoi, HCMC, Da Nang, Hai Phong, Can Tho, Hue, Nha Trang, Vung Tau, Qui Nhon, Thai Nguyen, etc.) at each hourly run via `air_pollution` endpoint. Use `air_pollution/forecast` for forecast ingestion. Use `air_pollution/history` for historical backfill.

### Secondary Source: WAQI (World Air Quality Index)

**Best for station-level granularity and cross-validation:**
- Free, no billing card required
- 11,000+ real monitoring stations globally, with meaningful Vietnam coverage
- Bounding-box query returns all Vietnam stations in one API call
- Includes weather data and forecast (3–8 days)
- Useful for validating OpenWeather model estimates

**Integration approach:** Use bounding-box query for Vietnam bbox (`/feed/geo:8.4;102.1;23.4;109.5/?token=...`) to enumerate all stations each run. Then fetch individual station detail data.

### Tertiary / Validation: US Embassy Hanoi

- Single-point high-quality reference for Hanoi only
- Useful for AQI calculation cross-validation
- Not suitable as a primary pipeline source

---

## Next Steps

1. **Register for OpenWeather API key** (free at openweathermap.org) — no billing required for free tier.
2. **Request WAQI API token** at aqicn.org/data-platform/token/ (free, email-based).
3. **Verify existing AQICN token** format — determine whether it is a WAQI token or IQAir token, and which API endpoint it maps to.
4. **Add ingestion jobs** for OpenWeather (current + forecast) to the existing `python_jobs/` structure, following the AQICN pattern.
5. **Add ingestion job** for WAQI using the bounding-box enumeration approach.
6. **Update `dag_ingest_hourly`** to include the new OpenWeather and WAQI measurement ingestion tasks.
7. **Cross-validate** OpenWeather model estimates against WAQI and AQICN station data to characterize accuracy.
