"""
Bootstrap OpenMetadata governance entities required by dbt metadata ingestion.

Creates or updates:
  - teams
  - domains

This lets dbt metadata such as `domain` and `owners` resolve directly during
OpenMetadata dbt ingestion without requiring a post-ingestion curation script.
"""

from __future__ import annotations

import base64
import os
from pathlib import Path
from typing import Any

import requests
import yaml

OM_URL = os.environ.get(
    "OM_URL",
    os.environ.get("OPENMETADATA_URL", "http://openmetadata:8585/api"),
).rstrip("/")
if not OM_URL.endswith("/api"):
    OM_URL += "/api"

OM_USER = os.environ.get("OM_ADMIN_USER")
OM_PASS = os.environ.get("OM_ADMIN_PASSWORD")
if not OM_USER or not OM_PASS:
    raise RuntimeError("OM_ADMIN_USER and OM_ADMIN_PASSWORD must be set")

DEFAULT_DEFINITIONS_PATH = (
    Path(__file__).resolve().parent / "governance_definitions.yml"
)

_token_cache: list[str] = []


def om_login() -> str:
    encoded_password = base64.b64encode(OM_PASS.encode("utf-8")).decode("utf-8")
    response = requests.post(
        f"{OM_URL}/v1/users/login",
        json={"email": OM_USER, "password": encoded_password},
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    response.raise_for_status()
    token = response.json()["accessToken"]
    _token_cache[:] = [token]
    return token


def get_headers() -> dict[str, str]:
    if not _token_cache:
        om_login()
    return {
        "Authorization": f"Bearer {_token_cache[0]}",
        "Content-Type": "application/json",
    }


def load_definitions() -> dict[str, Any]:
    config_path = Path(
        os.environ.get("OM_GOVERNANCE_CONFIG_PATH", DEFAULT_DEFINITIONS_PATH)
    )
    if not config_path.exists():
        raise FileNotFoundError(f"Governance config not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def om_put(path: str, payload: dict[str, Any]) -> requests.Response:
    response = requests.put(
        f"{OM_URL}{path}",
        json=payload,
        headers=get_headers(),
        timeout=30,
    )
    response.raise_for_status()
    return response


def om_post(path: str, payload: dict[str, Any]) -> requests.Response:
    response = requests.post(
        f"{OM_URL}{path}",
        json=payload,
        headers=get_headers(),
        timeout=30,
    )
    response.raise_for_status()
    return response


def om_get(path: str) -> requests.Response:
    response = requests.get(
        f"{OM_URL}{path}",
        headers=get_headers(),
        timeout=30,
    )
    response.raise_for_status()
    return response


def get_team_ref(name: str) -> dict[str, str]:
    payload = om_get(f"/v1/teams/name/{name}").json()
    return {
        "id": payload["id"],
        "type": "team",
        "name": payload["name"],
    }


def setup_team(team: dict[str, Any]) -> None:
    try:
        om_get(f"/v1/teams/name/{team['name']}")
        print(f"✅ Team ready: {team['name']}")
        return
    except requests.HTTPError as exc:
        if exc.response is None or exc.response.status_code != 404:
            raise

    payload = {
        "name": team["name"],
        "displayName": team.get("display_name", team["name"]),
        "teamType": team.get("team_type", "Group"),
        "description": team.get("description", ""),
    }
    om_post("/v1/teams", payload)
    print(f"✅ Team ready: {team['name']}")


def setup_domain(domain: dict[str, Any]) -> None:
    payload = {
        "name": domain["name"],
        "displayName": domain.get("display_name", domain["name"]),
        "domainType": domain.get("domain_type", "Aggregate"),
        "description": domain.get("description", ""),
    }

    owner_names = domain.get("owners", [])
    if owner_names:
        payload["owners"] = [get_team_ref(name) for name in owner_names]

    om_put("/v1/domains", payload)
    print(f"✅ Domain ready: {domain['name']}")


def setup_governance() -> None:
    definitions = load_definitions()
    teams = definitions.get("teams", [])
    domains = definitions.get("domains", [])

    print(f"Setting up {len(teams)} team(s) and {len(domains)} domain(s)...")

    for team in teams:
        setup_team(team)

    for domain in domains:
        setup_domain(domain)

    print("\n✅ Governance setup complete.")


if __name__ == "__main__":
    setup_governance()
