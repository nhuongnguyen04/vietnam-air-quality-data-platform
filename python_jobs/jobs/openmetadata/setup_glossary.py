"""
Setup OM glossaries and glossary terms from YAML configuration.
Idempotent - safe to run repeatedly.

Usage:
    OPENMETADATA_URL=http://localhost:8585/api \
    OM_ADMIN_USER=admin@open-metadata.org \
    OM_ADMIN_PASSWORD=admin \
    python python_jobs/jobs/openmetadata/setup_glossary.py
"""

import base64
import os
from pathlib import Path

import requests
import yaml

OM_URL = os.environ.get(
    "OM_URL",
    os.environ.get("OPENMETADATA_URL", "http://openmetadata:8585/api"),
).rstrip('/')
if not OM_URL.endswith('/api'):
    OM_URL += '/api'
OM_USER = os.environ.get("OM_ADMIN_USER")
OM_PASS = os.environ.get("OM_ADMIN_PASSWORD")
if not OM_USER or not OM_PASS:
    raise RuntimeError("OM_ADMIN_USER and OM_ADMIN_PASSWORD must be set")

DEFAULT_DEFINITIONS_PATH = (
    Path(__file__).resolve().parent / "glossary_definitions.yml"
)

_token_cache: list[str] = []


def om_login() -> str:
    b64_pass = base64.b64encode(OM_PASS.encode("utf-8")).decode("utf-8")
    r = requests.post(
        f"{OM_URL}/v1/users/login",
        json={"email": OM_USER, "password": b64_pass},
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    r.raise_for_status()
    token = r.json()["accessToken"]
    _token_cache.clear()
    _token_cache.append(token)
    return token


def get_token() -> str:
    if _token_cache:
        return _token_cache[0]
    return om_login()


def load_glossary_definitions() -> list[dict]:
    """Load glossary definitions from YAML config."""
    config_path = Path(
        os.environ.get("OM_GLOSSARY_CONFIG_PATH", DEFAULT_DEFINITIONS_PATH)
    )
    if not config_path.exists():
        raise FileNotFoundError(f"Glossary config not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    glossaries = payload.get("glossaries", [])
    if not glossaries:
        raise ValueError(f"No glossaries defined in {config_path}")

    return glossaries


def create_glossary(glossary: dict) -> str:
    """Create or get an existing glossary. Returns glossary ID."""
    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    glossary_name = glossary["name"]
    glossary_display_name = glossary.get("display_name", glossary_name)
    glossary_description = glossary.get("description", "")

    resp = requests.get(
        f"{OM_URL}/v1/glossaries/name/{glossary_name}",
        headers=headers,
        timeout=15,
    )
    if resp.ok:
        glossary_id = resp.json()["id"]
        print(f"ℹ️  Glossary '{glossary_name}' already exists, id={glossary_id}")
        return glossary_id

    user_resp = requests.get(
        f"{OM_URL}/v1/users/name/admin",
        headers=headers,
        timeout=15,
    )
    owner_id = user_resp.json()["id"] if user_resp.ok else None

    create_payload = {
        "name": glossary_name,
        "displayName": glossary_display_name,
        "description": glossary_description,
    }
    if owner_id:
        create_payload["owners"] = [{"id": owner_id, "type": "user"}]

    resp = requests.post(
        f"{OM_URL}/v1/glossaries",
        headers=headers,
        json=create_payload,
        timeout=30,
    )
    if resp.status_code == 409:
        resp = requests.get(
            f"{OM_URL}/v1/glossaries/name/{glossary_name}",
            headers=headers,
            timeout=15,
        )
        resp.raise_for_status()
        glossary_id = resp.json()["id"]
    else:
        resp.raise_for_status()
        glossary_id = resp.json()["id"]

    print(f"✅ Created glossary '{glossary_name}', id={glossary_id}")
    return glossary_id


def create_terms(glossary_name: str, glossary: dict):
    """Create glossary terms for a single glossary."""
    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    for term in glossary.get("terms", []):
        term_payload = {
            "name": term["name"],
            "displayName": term.get("display_name", term["name"]),
            "description": term["description"],
            "glossary": glossary_name,
            "mutuallyExclusive": False,
        }
        term_resp = requests.post(
            f"{OM_URL}/v1/glossaryTerms",
            headers=headers,
            json=term_payload,
            timeout=30,
        )
        error_text = term_resp.text.lower()
        if term_resp.status_code == 409 or (
            term_resp.status_code == 400 and "already exists" in error_text
        ):
            print(f"  ⏭ Term '{term['name']}' already exists")
        elif term_resp.ok:
            print(f"  ✅ Created term: {term['name']}")
        else:
            print(f"  ❌ Failed to create {term['name']}: {term_resp.text}")


def setup_glossary():
    """Main entry point - create all configured glossaries and terms."""
    glossaries = load_glossary_definitions()
    print(f"Setting up {len(glossaries)} configured glossary(ies)...")

    for glossary in glossaries:
        glossary_name = glossary["name"]
        print(f"\nSetting up glossary: {glossary_name}")
        create_glossary(glossary)
        terms = glossary.get("terms", [])
        print(f"Creating {len(terms)} glossary terms for {glossary_name}...")
        create_terms(glossary_name, glossary)

    print("\n✅ Glossary setup complete.")


if __name__ == "__main__":
    setup_glossary()
