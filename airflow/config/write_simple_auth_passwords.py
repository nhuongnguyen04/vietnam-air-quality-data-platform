#!/usr/bin/env python3
"""Write Airflow 3 Simple Auth Manager passwords from environment variables."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: write_simple_auth_passwords.py <passwords-file>", file=sys.stderr)
        return 2

    username = os.environ.get("AIRFLOW_ADMIN_USERNAME", "admin").strip()
    password = os.environ.get("AIRFLOW_ADMIN_PASSWORD", "")

    if not username:
        print("AIRFLOW_ADMIN_USERNAME must not be empty", file=sys.stderr)
        return 1
    if any(delimiter in username for delimiter in (":", ",")):
        print("AIRFLOW_ADMIN_USERNAME must not contain ':' or ','", file=sys.stderr)
        return 1
    if not password:
        print("AIRFLOW_ADMIN_PASSWORD must not be empty", file=sys.stderr)
        return 1

    passwords = {"admin": "admin"}
    passwords[username] = password

    output_path = Path(sys.argv[1])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(passwords) + "\n", encoding="utf-8")
    print(f"Wrote Airflow Simple Auth password file: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
