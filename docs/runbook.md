# Air Quality Platform — Operational Runbook

**Version:** 1.0 (Phase 5)
**Maintained by:** air-quality-team
**Last updated:** 2026-04-08

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Vietnam Air Quality Platform                        │
│                          End-to-End Data Pipeline                            │
└─────────────────────────────────────────────────────────────────────────────┘

INGESTION LAYER
───────────────
┌──────────────┐  ┌──────────────────┐  ┌────────────────────┐  ┌────────────┐
│ AQICN API   │  │ OpenWeather API  │  │ Sensors.Community  │  │  (future) │
│ api.waqi.info│  │ openweathermap   │  │ api.luftdaten.info │  │  MONRE     │
└──────┬───────┘  └───────┬──────────┘  └─────────┬──────────┘  └────────────┘
       │                  │                        │
       ▼                  ▼                        ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                        ClickHouse: air_quality                               │
│  raw_aqicn_measurements  |  raw_openweather_measurements  |  raw_sensorscm   │
│                        ReplacingMergeTree (dedup by ingest_time)            │
└──────────────────────────────────────┬───────────────────────────────────────┘
                                       │
TRANSFORM LAYER
───────────────
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                           dbt Transform                                      │
│  staging (stg_aqicn__, stg_openweather__, stg_sensorscm__)                   │
│    → intermediate (int_unified__, int_aqi_calculations__)                     │
│    → marts (fct_hourly_aqi, fct_daily_aqi_summary, dim_locations)           │
│    → analytics (mart_air_quality__dashboard, mart_air_quality__alerts)       │
│                                                                              │
│  Triggered by: dag_ingest_hourly → dag_transform (Airflow)                   │
└──────────────────────────────────────┬───────────────────────────────────────┘
                                       │
VISUALIZATION + MONITORING
─────────────────────────
                                       ▼
         ┌──────────────────────────────┴───────────────────────────────┐
         │                     Airflow (scheduling)                     │
         │  dag_ingest_hourly  |  dag_transform  |  dag_sensorscm_poll │
         │  dag_weekly_report  |  dag_smoke_test  |  dag_metadata_update│
         └──────────────────────────────┬───────────────────────────────┘
                                        │
                    ┌────────────────────┴────────────────────┐
                    ▼                                         ▼
         ┌──────────────────┐                   ┌──────────────────────────┐
         │  Grafana :3000   │                   │  Streamlit :8501        │
         │  (alerts + ops)  │                   │  (5-page dashboard)     │
         └────────┬─────────┘                   └──────────────────────────┘
                  │
                  │  🔴 CRITICAL: / 🟡 WARNING:
                  ▼
         ┌──────────────────────────────────────────────────────────────────┐
         │                    Telegram Bot API                              │
         │  All alerts routed to single chat (chat_id: 5602934306)          │
         │  Bot token: ${TELEGRAM_BOT_TOKEN}                               │
         └──────────────────────────────────────────────────────────────────┘

ALERT FLOW
──────────
  Grafana Alert Rules (evaluate every 1 min)
    │
    ├─ AQI > 200      → 🔴 CRITICAL   → Telegram
    ├─ AQI > 150      → 🟡 WARNING    → Telegram
    ├─ PM2.5 > 75µg   → 🟡 WARNING    → Telegram
    ├─ Source Diverg. → 🟡 WARNING    → Telegram
    ├─ DAG Failure    → 🔴 CRITICAL   → Telegram
    └─ ClickHouse Down→ 🔴 CRITICAL   → Telegram

  Airflow DAG (scheduled)
    └─ dag_weekly_report (Mon 02:00 UTC = 09:00 UTC+7)
         └─ 📊 Weekly Telegram Report (city AQI, top 5, trend, pollutants)

METADATA & CATALOG
───────────────────
         ┌──────────────────┐
         │ OpenMetadata :8585 │
         └────────┬─────────┘
                  │  catalog curation, lineage, glossary
                  ▼
         ┌──────────────────────────┐
         │  DAG: dag_openmetadata_  │
         │  curation (hourly, :35)  │
         └──────────────────────────┘
```

---

## Alert Routing Guide

### Alert Types and Destinations

| UID | Alert | Threshold | Severity | Destination | Re-alert |
|-----|-------|-----------|----------|-------------|---------|
| `aqi-critical-200` | AQI Critical | AQI > 200 | 🔴 CRITICAL | Telegram | Every 1h |
| `aqi-warning-150` | AQI Warning | AQI > 150 | 🟡 WARNING | Telegram | Every 1h |
| `pm25-warning-75` | PM2.5 Warning | PM2.5 > 75 µg/m³ | 🟡 WARNING | Telegram | Every 1h |
| `multi-source-divergence` | Multi-Source Divergence | \|AQICN−OW\| > 50 | 🟡 WARNING | Telegram | Every 1h |
| `dag-failure-critical` | DAG Failure | any DAG fails | 🔴 CRITICAL | Telegram | Every 1h |
| `clickhouse-down-critical` | ClickHouse Down | ping fails | 🔴 CRITICAL | Telegram | Every 1h |
| `data-freshness-warning` | Data Freshness | lag > 3h | 🟡 WARNING | Grafana only | 30 min |
| `station-stale-warning` | Station Stale | no data > 2h | 🟡 WARNING | Grafana only | 30 min |

### Severity Meanings

- **🔴 CRITICAL**: Immediate action required. Pipeline is broken or AQI is hazardous.
- **🟡 WARNING**: Degraded service or AQI unhealthy for sensitive groups. Monitor closely.

### Receiving Alerts

1. Add your Telegram account to the bot chat (chat ID: `5602934306`)
2. The bot will forward all alerts automatically
3. No configuration needed — Telegram is the single notification channel

---

## DAG Troubleshooting Guide

### How to Diagnose a Failing DAG

**Step 1: Identify the failing DAG**
```bash
docker compose logs airflow-scheduler --tail=100 | grep -i "failed\|error"
```

**Step 2: Check DAG status in Airflow UI**
- Open: http://localhost:8090
- Find the DAG → check last 5 runs

**Step 3: View task-level logs**
```bash
# Get task instance logs
docker compose exec airflow-webserver \
  airflow tasks states-for-dag-run <dag-id> <run-id>

# View log for specific task
docker compose exec airflow-webserver \
  airflow tasks render \
  dag_ingest_hourly <task-id> <execution-date>
```

**Step 4: Common failure patterns**

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| Ingestion task fails with HTTP 429 | Rate limit exceeded | Wait 60s, DAG auto-retries |
| Ingestion task fails with HTTP 401/403 | Invalid API key | Check AQICN/API key env vars |
| dbt run fails on stg model | Schema changed upstream | Re-audit source columns |
| dbt run fails on mart model | ClickHouse OOM | Check `docker stats`, increase memory |
| Grafana panel shows no data | Table empty or schema mismatch | Run `dag_transform` manually |
| Alert rule shows "No Data" | Source table missing | Check `fct_hourly_aqi` has data |

**Step 5: Re-run a failed DAG**
```bash
docker compose exec airflow-webserver \
  airflow dags trigger <dag-id> \
  --run-id "manual-$(date +%Y%m%d-%H%M%S)"
```

**Step 6: Clear a stuck DAG run**
```bash
docker compose exec airflow-webserver \
  airflow dags clear <dag-id> \
  --dag_run_id <run-id> \
  --yes
```

---

## Weekly Report Guide

### Triggering Manually

```bash
# Trigger weekly report DAG immediately (bypass schedule)
docker compose exec airflow-webserver \
  airflow dags trigger dag_weekly_report
```

### Verifying Report Sent

Check Telegram for the 📊 Air Quality Weekly Report message. If not received:

1. Check DAG ran successfully:
   ```bash
   docker compose logs airflow-scheduler --tail=50 | grep dag_weekly_report
   ```

2. Check Telegram credentials:
   ```bash
   docker compose exec airflow-webserver \
     printenv TELEGRAM_BOT_TOKEN
   docker compose exec airflow-webserver \
     printenv TELEGRAM_CHAT_ID
   ```

3. Test Telegram bot directly:
   ```python
   import requests
   TOKEN = "<your-bot-token>"
   resp = requests.get(f"https://api.telegram.org/bot{TOKEN}/getMe")
   print(resp.json())
   ```

---

## Smoke Test Guide

### Running the Smoke Test

```bash
# Trigger smoke test DAG
docker compose exec airflow-webserver \
  airflow dags trigger dag_smoke_test
```

**Expected outcome:**
1. Task `insert_test_alert` → inserts row with AQI=250 into `mart_air_quality__alerts`
2. Task `verify_alert_created` → confirms row readable
3. Task `cleanup_test_alert` → removes test row

If `verify_alert_created` fails → `mart_air_quality__alerts` table or write path is broken.

---

## Rollback Procedures

### Rollback Grafana Alert Rules

If new alert rules cause Grafana to crash or misfire:

```bash
# 1. Restore the previous version of alert-rules.yml
git checkout HEAD~1 -- grafana/provisioning/alerting/alert-rules.yml

# 2. Restart Grafana to reload rules
docker compose restart grafana

# 3. Verify Grafana is up
curl http://localhost:3000/api/health
```

### Rollback dbt Mart Models

If mart tables have wrong data after `dag_transform`:

```bash
# 1. Check which model failed
docker compose logs airflow-scheduler --tail=50 | grep "dbt run"

# 2. Re-run only the broken model
docker compose exec airflow-webserver \
  airflow tasks run dag_transform \
  <task-id> --local \
  -- Execution date of failed run

# 3. If data is corrupted, full refresh (warning: expensive)
docker compose exec airflow-scheduler \
  dbt run --profiles-dir /opt/dbt/dbt_tranform \
  --target production --full-refresh \
  --select <broken_model>
```

### Disable All Telegram Alerts (Emergency)

If Telegram bot is malfunctioning or chat is flooded:

1. **Via Grafana UI** (fastest):
   - Go to http://localhost:3000 → Alerting
   - Find each rule → mute button

2. **Via contact point** (temporary):
   ```bash
   # Comment out contact point in alert-rules.yml
   # contactPoints: [telegram-critical]  → # contactPoints: [telegram-critical]
   docker compose restart grafana
   ```

3. **Via Docker** (if Grafana is down):
   ```bash
   # Remove Grafana alerting volume (rules reload from git on restart)
   docker compose stop grafana
   docker volume rm vietnam-air-quality-data-platform_grafana-data
   docker compose up -d grafana
   ```

---

## Contact Information

| Role | Contact |
|------|---------|
| On-call engineer | See rotation spreadsheet |
| Data platform lead | Internal Slack #data-ops |
| Telegram chat | chat ID 5602934306 |

---

*For OpenMetadata catalog governance, see `docs/OPENMETADATA_GOVERNANCE.md`*
*For stack versions and dependencies, see `docs/STACK.md`*
