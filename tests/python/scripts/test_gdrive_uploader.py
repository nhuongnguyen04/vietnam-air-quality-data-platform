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
