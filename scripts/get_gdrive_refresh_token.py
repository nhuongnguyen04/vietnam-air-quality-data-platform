#!/usr/bin/env python3
"""
Script lấy OAuth2 Refresh Token cho Google Drive API.
Chạy lần đầu để authenticate và lưu refresh_token.

Cách dùng:
1. Download client_secret.json từ Google Cloud Console
   (Credentials > OAuth 2.0 Client ID > Web application / Desktop app)
2. Đặt client_secret.json vào thư mục scripts/
3. Chạy: python scripts/get_gdrive_refresh_token.py
4. Làm theo hướng dẫn trên trình duyệt (hoặc dùng local server)
5. Copy refresh_token được in ra và thêm vào GitHub Secrets

Lưu ý: Refresh token sẽ bị revoke nếu bạn:
- Đổi mật khẩu Google account
- Thu hồi quyền truy cập
- Tạo new credentials sau khi đã có credentials cũ
"""

import json
import os
import sys
from pathlib import Path

# Thêm project root vào path để import được common modules (nếu cần)
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from google_auth_oauthlib.flow import InstalledAppFlow
except ImportError:
    print("Lỗi: Chưa cài google-auth-oauthlib. Cài đặt với:")
    print("  pip install google-auth-oauthlib google-api-python-client")
    sys.exit(1)

SCOPES = ["https://www.googleapis.com/auth/drive"]
TOKEN_FILE = Path(__file__).parent / "gdrive_token.json"
CLIENT_SECRET_FILE = Path(__file__).parent / "client_secret.json"


def main():
    # 1. Kiểm tra client_secret.json tồn tại
    if not CLIENT_SECRET_FILE.exists():
        print(f"Lỗi: Không tìm thấy {CLIENT_SECRET_FILE}")
        print()
        print("Hướng dẫn lấy client_secret.json:")
        print("  1. Truy cập https://console.cloud.google.com/apis/credentials")
        print("  2. Tạo OAuth 2.0 Client ID (Desktop app hoặc Web application)")
        print("  3. Download JSON và đổi tên thành client_secret.json")
        print("  4. Đặt vào thư mục scripts/")
        sys.exit(1)

    # 2. Chạy OAuth flow
    print("Khởi tạo OAuth flow...")
    print(f"  Client secret: {CLIENT_SECRET_FILE}")
    print(f"  Scopes: {SCOPES}")
    print()

    # InstalledAppFlow.run_local_server sẽ:
    # - Mở trình duyệt để user authorize
    # - Listen local server để nhận callback
    # - Exchange authorization code lấy credentials (bao gồm refresh_token)
    flow = InstalledAppFlow.from_client_secrets_file(
        str(CLIENT_SECRET_FILE),
        scopes=SCOPES
    )

    print("Đang mở trình duyệt để authorize...")
    print("(Nếu trình duyệt không tự mở, hãy truy cập URL được in ra bên dưới)")
    print()

    try:
        # run_local_server: tự mở browser, listen callback, tự động stop
        # port=0: chọn available port tự động
        credentials = flow.run_local_server(port=0, prompt="consent", access_type="offline")
    except Exception as e:
        print(f"Lỗi khi chạy OAuth flow: {e}")
        sys.exit(1)

    # 3. Lấy thông tin từ credentials
    refresh_token = credentials.refresh_token
    client_id = credentials.client_id
    client_secret = credentials.client_secret

    if not refresh_token:
        print("Lỗi: Không lấy được refresh_token. Có thể cần thêm 'prompt=consent' khi run_local_server.")
        print("Thử chạy lại script...")
        sys.exit(1)

    # 4. Lưu vào file token
    token_data = {
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }

    TOKEN_FILE.write_text(json.dumps(token_data, indent=2))
    print(f"✓ Đã lưu token vào {TOKEN_FILE}")

    # 5. In ra thông tin để copy vào GitHub Secrets
    print()
    print("=" * 60)
    print("THÔNG TIN CẦN THÊM VÀO GITHUB SECRETS:")
    print("=" * 60)
    print()
    print(f"GDRIVE_CLIENT_ID={client_id}")
    print(f"GDRIVE_CLIENT_SECRET={client_secret}")
    print(f"GDRIVE_REFRESH_TOKEN={refresh_token}")
    print()
    print("=" * 60)

    # 6. Test xác thực
    print("Testing OAuth token...")
    try:
        from google.api_core.optional_imports import import_library
        from google.auth.transport.requests import Request

        credentials.refresh(Request())
        print(f"✓ Token hợp lệ! (scopes: {credentials.scopes})")
    except Exception as e:
        print(f"✗ Token không hợp lệ: {e}")
        print("  Vui lòng kiểm tra lại Google Cloud Console credentials.")

    print()
    print("Xong! Copy 3 giá trị ở trên vào GitHub repo → Settings → Secrets → Actions")


if __name__ == "__main__":
    main()
