#!/usr/bin/env python3
import os
import sys
from google_auth_oauthlib.flow import InstalledAppFlow

# Drive Full Access Scope
SCOPES = ['https://www.googleapis.com/auth/drive']

def main():
    print("=== Google Drive OAuth 2.0 Refresh Token Generator ===")
    print("Bạn cần có Client ID và Client Secret từ Google Cloud Console (Desktop App).")
    print("-" * 50)
    
    client_id = input("1. Nhập Client ID: ").strip()
    client_secret = input("2. Nhập Client Secret: ").strip()
    
    if not client_id or not client_secret:
        print("Lỗi: Bạn phải nhập đủ Client ID và Client Secret.")
        sys.exit(1)

    client_config = {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    }

    try:
        # Chạy quy trình xác thực cục bộ
        flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
        creds = flow.run_local_server(port=0)

        print("-" * 50)
        print("XÁC THỰC THÀNH CÔNG!")
        print("-" * 50)
        print(f"GDRIVE_CLIENT_ID: {client_id}")
        print(f"GDRIVE_CLIENT_SECRET: {client_secret}")
        print(f"GDRIVE_REFRESH_TOKEN: {creds.refresh_token}")
        print("-" * 50)
        print("BƯỚC TIẾP THEO:")
        print("1. Copy 3 giá trị trên vào mục Settings > Secrets của GitHub.")
        print("2. Copy 3 giá trị trên vào file .env ở local của bạn.")
        print("-" * 50)

    except Exception as e:
        print(f"Lỗi trong quá trình xác thực: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
