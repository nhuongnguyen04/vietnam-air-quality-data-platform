from datetime import datetime, timezone

from python_jobs.jobs.waqi.ingest_measurements import parse_waqi_timestamp, process_results


def test_parse_waqi_timestamp_uses_iso_offset_as_utc_instant():
    timestamp = parse_waqi_timestamp(
        {
            "s": "2026-06-08 15:00:00",
            "tz": "+07:00",
            "v": 1780930800,
            "iso": "2026-06-08T15:00:00+07:00",
        }
    )

    assert timestamp == datetime(2026, 6, 8, 8, 0, tzinfo=timezone.utc)


def test_parse_waqi_timestamp_uses_timezone_offset_when_iso_missing():
    timestamp = parse_waqi_timestamp(
        {
            "s": "2026-06-08 15:00:00",
            "tz": "+07:00",
        }
    )

    assert timestamp == datetime(2026, 6, 8, 8, 0, tzinfo=timezone.utc)


def test_process_results_writes_real_utc_timestamp():
    records = process_results(
        [
            {
                "station_name": "Ba Dinh, Hanoi, Hanoi, Vietnam",
                "success": True,
                "data": {
                    "aqi": 229,
                    "iaqi": {"pm25": {"v": 229}},
                    "time": {"iso": "2026-06-08T15:00:00+07:00"},
                },
            }
        ],
        batch_id="test_batch",
    )

    assert records[0]["timestamp_utc"] == datetime(2026, 6, 8, 8, 0, tzinfo=timezone.utc)
