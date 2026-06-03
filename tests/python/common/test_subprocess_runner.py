import os
import sys
import pytest
from unittest import mock
from python_jobs.common.config import require_env, get_clickhouse_env_vars
from python_jobs.common.subprocess_runner import run_python_job

def test_require_env_success():
    with mock.patch.dict(os.environ, {"TEST_VAR_XYZ": "value123"}):
        assert require_env("TEST_VAR_XYZ") == "value123"

def test_require_env_missing():
    with mock.patch.dict(os.environ, {}):
        if "TEST_VAR_XYZ" in os.environ:
            del os.environ["TEST_VAR_XYZ"]
        with pytest.raises(RuntimeError) as exc_info:
            require_env("TEST_VAR_XYZ")
        assert "TEST_VAR_XYZ environment variable is required" in str(exc_info.value)

def test_require_env_empty():
    with mock.patch.dict(os.environ, {"TEST_VAR_XYZ": ""}):
        with pytest.raises(RuntimeError) as exc_info:
            require_env("TEST_VAR_XYZ")
        assert "TEST_VAR_XYZ environment variable is required" in str(exc_info.value)

def test_get_clickhouse_env_vars():
    env_mock = {
        "CLICKHOUSE_HOST": "my-host",
        "CLICKHOUSE_PORT": "1234",
        "CLICKHOUSE_USER": "my-user",
        "CLICKHOUSE_PASSWORD": "my-password",
        "CLICKHOUSE_DB": "my-db",
    }
    with mock.patch.dict(os.environ, env_mock):
        vars_dict = get_clickhouse_env_vars()
        assert vars_dict["CLICKHOUSE_HOST"] == "my-host"
        assert vars_dict["CLICKHOUSE_PORT"] == "1234"
        assert vars_dict["CLICKHOUSE_USER"] == "my-user"
        assert vars_dict["CLICKHOUSE_PASSWORD"] == "my-password"
        assert vars_dict["CLICKHOUSE_DB"] == "my-db"

def test_run_python_job_success(tmp_path):
    # Create a small script that exits with 0 and prints something
    script = tmp_path / "hello.py"
    script.write_text("print('hello stdout')\n")
    
    result = run_python_job(str(script))
    assert result.returncode == 0
    assert "hello stdout" in result.stdout

def test_run_python_job_failure(tmp_path):
    # Create a small script that exits with 1
    script = tmp_path / "fail.py"
    script.write_text("import sys; print('error stderr', file=sys.stderr); sys.exit(1)\n")
    
    with pytest.raises(RuntimeError) as exc_info:
        run_python_job(str(script))
    assert "Command failed with return code 1" in str(exc_info.value)

def test_run_python_job_failure_no_raise(tmp_path):
    # Create a small script that exits with 1
    script = tmp_path / "fail.py"
    script.write_text("import sys; sys.exit(2)\n")
    
    result = run_python_job(str(script), raise_on_error=False)
    assert result.returncode == 2
