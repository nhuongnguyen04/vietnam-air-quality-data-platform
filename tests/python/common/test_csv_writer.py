import csv

from python_jobs.common.csv_writer import CSVWriter


def test_write_batch_uses_unique_chunk_filenames(tmp_path):
    writer = CSVWriter(output_dir=str(tmp_path))
    records = [{"ward_code": "00001", "parameter": "pm25", "value": 12.3}]

    assert writer.write_batch("raw_openweather_measurements", records, source="openweather") == 1
    assert writer.write_batch("raw_openweather_measurements", records, source="openweather") == 1

    files = sorted(tmp_path.glob("openweather_meas_*.csv"))
    assert len(files) == 2
    assert files[0].name != files[1].name

    for path in files:
        with path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        assert len(rows) == 1
        assert rows[0]["ward_code"] == "00001"
