from pathlib import Path

import pytest

from scripts import gdrive_uploader


@pytest.mark.unit
def test_main_returns_failure_when_any_upload_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    csv_file = tmp_path / "aqiin_meas_20260511.csv"
    csv_file.write_text("id,value\n1,42\n")

    monkeypatch.setattr(gdrive_uploader, "DRIVE_ROOT_ID", "root-folder")
    monkeypatch.setattr(gdrive_uploader, "LOCAL_LANDING_ZONE", str(tmp_path))
    monkeypatch.setattr(gdrive_uploader, "get_drive_service", lambda: object())

    def fail_upload(_service: object, _local_path: Path) -> None:
        raise RuntimeError("upload rejected")

    monkeypatch.setattr(gdrive_uploader, "upload_file", fail_upload)

    assert gdrive_uploader.main() == 1
    assert csv_file.exists()


@pytest.mark.unit
def test_main_returns_success_when_uploads_succeed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    csv_file = tmp_path / "aqiin_meas_20260511.csv"
    csv_file.write_text("id,value\n1,42\n")

    monkeypatch.setattr(gdrive_uploader, "DRIVE_ROOT_ID", "root-folder")
    monkeypatch.setattr(gdrive_uploader, "LOCAL_LANDING_ZONE", str(tmp_path))
    monkeypatch.setattr(gdrive_uploader, "get_drive_service", lambda: object())
    monkeypatch.setattr(gdrive_uploader, "upload_file", lambda _service, _path: "file-id")

    assert gdrive_uploader.main() == 0
    assert not csv_file.exists()


@pytest.mark.unit
def test_get_target_folder_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gdrive_uploader, "DRIVE_ROOT_ID", "root_folder")

    created_folders = []

    def mock_find_or_create_folder(service, name, parent_id):
        dummy_id = f"id_{parent_id}_{name}"
        created_folders.append((name, parent_id, dummy_id))
        return dummy_id

    monkeypatch.setattr(gdrive_uploader, "find_or_create_folder", mock_find_or_create_folder)

    # Test aqiin
    created_folders.clear()
    res = gdrive_uploader.get_target_folder_id(None, "aqiin_meas_20260608_1504_0001.csv")
    assert res == "id_id_root_folder_landing_zone_aqi_in"
    assert created_folders == [
        ("landing_zone", "root_folder", "id_root_folder_landing_zone"),
        ("aqi_in", "id_root_folder_landing_zone", "id_id_root_folder_landing_zone_aqi_in")
    ]

    # Test openweather measurements
    created_folders.clear()
    res = gdrive_uploader.get_target_folder_id(None, "openweather_meas_20260608.csv")
    assert res == "id_id_id_root_folder_landing_zone_openweather_measurements"
    assert created_folders == [
        ("landing_zone", "root_folder", "id_root_folder_landing_zone"),
        ("openweather", "id_root_folder_landing_zone", "id_id_root_folder_landing_zone_openweather"),
        ("measurements", "id_id_root_folder_landing_zone_openweather", "id_id_id_root_folder_landing_zone_openweather_measurements")
    ]

    # Test openweather weather
    created_folders.clear()
    res = gdrive_uploader.get_target_folder_id(None, "openweather_weat_20260608.csv")
    assert res == "id_id_id_root_folder_landing_zone_openweather_weather"

    # Test tomtom
    created_folders.clear()
    res = gdrive_uploader.get_target_folder_id(None, "tomtom_traf_20260608.csv")
    assert res == "id_id_id_root_folder_landing_zone_tomtom_traffic"

    # Test waqi measurements
    created_folders.clear()
    res = gdrive_uploader.get_target_folder_id(None, "waqi_meas_20260608_1504_0001.csv")
    assert res == "id_id_id_root_folder_landing_zone_waqi_measurements"
    assert created_folders == [
        ("landing_zone", "root_folder", "id_root_folder_landing_zone"),
        ("waqi", "id_root_folder_landing_zone", "id_id_root_folder_landing_zone_waqi"),
        ("measurements", "id_id_root_folder_landing_zone_waqi", "id_id_id_root_folder_landing_zone_waqi_measurements")
    ]

    # Test waqi stations
    created_folders.clear()
    res = gdrive_uploader.get_target_folder_id(None, "waqi_stat_20260608_1504_0001.csv")
    assert res == "id_id_id_root_folder_landing_zone_waqi_stations"
    assert created_folders == [
        ("landing_zone", "root_folder", "id_root_folder_landing_zone"),
        ("waqi", "id_root_folder_landing_zone", "id_id_root_folder_landing_zone_waqi"),
        ("stations", "id_id_root_folder_landing_zone_waqi", "id_id_id_root_folder_landing_zone_waqi_stations")
    ]

