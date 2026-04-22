"""Unit tests for the shared HTTP API client."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import requests

from python_jobs.common.api_client import APIClient


@pytest.mark.unit
def test_build_url_filters_none_values() -> None:
    client = APIClient(base_url="https://example.com/api", token="secret")

    url = client._build_url("/measurements", {"city": "hanoi", "page": None, "limit": 100})

    assert url == "https://example.com/api/measurements?city=hanoi&limit=100"
    client.close()


@pytest.mark.unit
def test_get_injects_appid_token_into_query_params(monkeypatch: pytest.MonkeyPatch) -> None:
    limiter = MagicMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"ok": True}
    response.raise_for_status.return_value = None

    client = APIClient(
        base_url="https://api.openweathermap.org/data/2.5",
        token="weather-token",
        auth_header_name="appid",
        rate_limiter=limiter,
    )
    monkeypatch.setattr(client.session, "request", MagicMock(return_value=response))

    payload = client.get("/air_pollution", params={"lat": 21.0, "lon": 105.8})

    assert payload == {"ok": True}
    limiter.acquire.assert_called_once()
    call = client.session.request.call_args
    assert call.kwargs["url"].endswith("/air_pollution?lat=21.0&lon=105.8&appid=weather-token")
    assert "appid" not in call.kwargs["headers"]
    client.close()


@pytest.mark.unit
def test_http_error_marks_token_manager_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    token_manager = MagicMock()
    token_manager.get_token_and_limiter.return_value = ("rotated-token", None, 3)

    response = MagicMock()
    response.status_code = 503
    response.text = "upstream unavailable"
    response.raise_for_status.side_effect = requests.HTTPError("503 Service Unavailable")

    client = APIClient(
        base_url="https://example.com/api",
        token_manager=token_manager,
        auth_header_name="Authorization",
    )
    monkeypatch.setattr(client.session, "request", MagicMock(return_value=response))

    with pytest.raises(requests.HTTPError):
        client.get("/health")

    token_manager.mark_failed.assert_called_once_with(3, 503)
    client.close()
