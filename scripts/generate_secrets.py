#!/usr/bin/env python3
"""
Python script to generate cryptographically secure secrets and replace placeholders in .env file.
Uses only standard library.
"""
import base64
import os
import re
import secrets

def generate_fernet_key() -> str:
    # A Fernet key is 32 base64url-encoded bytes
    return base64.urlsafe_b64encode(os.urandom(32)).decode()

def main():
    env_file = ".env"
    env_example = ".env.example"

    if not os.path.exists(env_example):
        print(f"[-] Error: {env_example} not found!")
        return

    # Check if .env already exists
    if os.path.exists(env_file):
        print(f"[*] Warning: {env_file} already exists. Creating backup as .env.bak")
        os.rename(env_file, f"{env_file}.bak")

    # Read .env.example
    with open(env_example, "r", encoding="utf-8") as f:
        lines = f.readlines()

    new_lines = []
    generated = {}

    for line in lines:
        stripped = line.strip()
        # Look for active configuration lines
        if stripped and not stripped.startswith("#") and "=" in line:
            key, val = stripped.split("=", 1)
            key = key.strip()
            val = val.strip()

            # Replace specific placeholder templates with secure randoms
            if key == "CLICKHOUSE_PASSWORD" and "your_clickhouse_password_here" in val:
                sec = secrets.token_urlsafe(16)
                line = line.replace(val, sec)
                generated[key] = sec
            elif key == "AIRFLOW_ADMIN_PASSWORD" and "replace-with-a-strong-password" in val:
                sec = secrets.token_urlsafe(12)
                line = line.replace(val, sec)
                generated[key] = sec
            elif key == "AIRFLOW__CORE__FERNET_KEY" and "replace-with-a-generated-fernet-key" in val:
                sec = generate_fernet_key()
                line = line.replace(val, sec)
                generated[key] = sec
            elif key in ["AIRFLOW_API_SECRET_KEY", "AIRFLOW_API_AUTH_JWT_SECRET", "AIRFLOW_WEBSERVER_SECRET_KEY", "TEXT_TO_SQL_PREVIEW_SECRET"] and "change-me" in val:
                sec = secrets.token_hex(32)
                line = line.replace(val, sec)
                generated[key] = sec
            elif key == "GF_SECURITY_ADMIN_PASSWORD" and "replace-with-a-grafana-admin-password" in val:
                sec = secrets.token_urlsafe(12)
                line = line.replace(val, sec)
                generated[key] = sec
            elif key == "TEXT_TO_SQL_CLICKHOUSE_PASSWORD" and "change-me" in val:
                sec = secrets.token_urlsafe(16)
                line = line.replace(val, sec)
                generated[key] = sec

        new_lines.append(line)

    # Write new .env file
    with open(env_file, "w", encoding="utf-8") as f:
        f.writelines(new_lines)

    print("[+] Successfully generated secure secrets and created .env file!")
    print("[*] The following keys were automatically randomized for maximum security:")
    for k in generated:
        # Hide the actual secret values from terminal outputs to prevent logs exposure
        print(f"  - {k}: [GENERATED SECURELY]")

if __name__ == "__main__":
    main()
