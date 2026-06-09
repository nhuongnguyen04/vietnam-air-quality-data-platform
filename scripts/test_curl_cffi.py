#!/usr/bin/env python3
import re
import sys
from curl_cffi import requests

def fetch_token():
    url = "https://www.aqi.in/vi/dashboard/vietnam/ha-noi/hanoi/hanoi-us-embassy"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    
    print(f"Fetching {url} using curl_cffi...")
    try:
        # We impersonate Chrome 120 (which will mimic Chrome's TLS Client Hello and HTTP/2 settings)
        r = requests.get(url, headers=headers, impersonate="chrome120", timeout=15)
        print(f"Response status code: {r.status_code}")
        
        html = r.text
        if "Just a moment" in html:
            print("❌ Stuck on Cloudflare Challenge page.")
            return None
            
        pattern = r'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9\.[A-Za-z0-9-_=]+\.[A-Za-z0-9-_=]+'
        matches = re.findall(pattern, html)
        
        if matches:
            print("✅ Successfully found JWT Token(s):")
            for m in set(matches):
                print(f"Token: {m[:30]}... ({len(m)} chars)")
            return matches[0]
        else:
            print("❌ No JWT token found in HTML. Snippet of HTML:")
            print(html[:500])
            return None
    except Exception as e:
        print(f"❌ Request failed with exception: {e}")
        return None

if __name__ == "__main__":
    token = fetch_token()
    if token:
        sys.exit(0)
    else:
        sys.exit(1)
