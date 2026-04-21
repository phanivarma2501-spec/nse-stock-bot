"""
Standalone Kite Connect login test.

Run locally to verify API key + password + TOTP all work end-to-end.
Prints access_token + a sample NIFTY LTP + first 3 option instruments.

Usage:
    set KITE_API_KEY=xxx
    set KITE_API_SECRET=xxx
    set KITE_USER_ID=EJK043
    set KITE_PASSWORD=xxx
    set KITE_TOTP_SECRET=JBSWY3DPEHPK3PXP...
    python kite_login_test.py
"""

import os
import sys
from urllib.parse import urlparse, parse_qs

try:
    import pyotp
    import requests
    from kiteconnect import KiteConnect
except ImportError as e:
    print(f"Missing package: {e}. Run: pip install kiteconnect pyotp requests")
    sys.exit(1)


API_KEY = os.environ.get("KITE_API_KEY", "").strip()
API_SECRET = os.environ.get("KITE_API_SECRET", "").strip()
USER_ID = os.environ.get("KITE_USER_ID", "").strip()
PASSWORD = os.environ.get("KITE_PASSWORD", "").strip()
TOTP_SECRET = os.environ.get("KITE_TOTP_SECRET", "").strip()


def _require(name, value):
    if not value:
        print(f"ERROR: {name} env var is not set.")
        sys.exit(1)


for n, v in [
    ("KITE_API_KEY", API_KEY),
    ("KITE_API_SECRET", API_SECRET),
    ("KITE_USER_ID", USER_ID),
    ("KITE_PASSWORD", PASSWORD),
    ("KITE_TOTP_SECRET", TOTP_SECRET),
]:
    _require(n, v)


def login_and_get_access_token() -> str:
    """Automated Kite login using TOTP. Returns access_token valid for the day."""
    kite = KiteConnect(api_key=API_KEY)
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
    })

    # Prime cookies by hitting the dashboard first
    session.get("https://kite.zerodha.com/")

    # Step 1: POST user_id + password -> request_id
    r = session.post(
        "https://kite.zerodha.com/api/login",
        data={"user_id": USER_ID, "password": PASSWORD},
    )
    print(f"  step1 status={r.status_code}")
    print(f"  step1 body (first 500 chars): {r.text[:500]}")
    print(f"  step1 response headers: {dict(r.headers)}")
    r.raise_for_status()
    body = r.json()
    if body.get("status") != "success":
        raise RuntimeError(f"Login step 1 failed: {body}")
    request_id = body["data"]["request_id"]
    print(f"[1/4] login OK, request_id={request_id[:10]}...")

    # Step 2: POST TOTP code
    totp_code = pyotp.TOTP(TOTP_SECRET).now()
    r = session.post(
        "https://kite.zerodha.com/api/twofa",
        data={
            "user_id": USER_ID,
            "request_id": request_id,
            "twofa_value": totp_code,
            "twofa_type": "totp",
            "skip_session": "",
        },
    )
    r.raise_for_status()
    body = r.json()
    if body.get("status") != "success":
        raise RuntimeError(f"TOTP step failed: {body}")
    print(f"[2/4] TOTP OK")

    # Step 3: Hit OAuth connect URL to obtain request_token
    # Kite redirects to our app's redirect_uri with ?request_token=...; requests
    # will fail to connect to that URL (usually localhost), but the redirect
    # chain captures it. Use allow_redirects=True and catch the connection error.
    try:
        r = session.get(f"https://kite.zerodha.com/connect/login?v=3&api_key={API_KEY}")
        # If redirect succeeded (e.g. redirect_uri is a real site), parse final URL
        final_url = r.url
    except requests.exceptions.ConnectionError as e:
        # Extract URL from the last request before connection failed
        # The failed request's URL has the request_token in its query
        final_url = e.request.url if e.request else None

    if not final_url:
        raise RuntimeError("Could not determine redirect URL with request_token")
    parsed = urlparse(final_url)
    qs = parse_qs(parsed.query)
    if "request_token" not in qs:
        raise RuntimeError(f"request_token not in redirect URL: {final_url}")
    request_token = qs["request_token"][0]
    print(f"[3/4] request_token acquired: {request_token[:10]}...")

    # Step 4: Exchange request_token + api_secret for access_token
    data = kite.generate_session(request_token, api_secret=API_SECRET)
    access_token = data["access_token"]
    print(f"[4/4] access_token acquired: {access_token[:10]}...")
    return access_token


def main():
    print("=== Kite TOTP login test ===\n")
    access_token = login_and_get_access_token()

    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(access_token)

    # Sanity check: fetch NIFTY spot LTP
    print("\n=== Fetching NIFTY 50 LTP ===")
    ltp = kite.ltp(["NSE:NIFTY 50"])
    print(f"NSE:NIFTY 50 LTP = {ltp}")

    # Sanity check: get first 3 NIFTY options from instruments dump
    print("\n=== Fetching F&O instruments for NIFTY (first 3) ===")
    instruments = kite.instruments("NFO")
    nifty_opts = [i for i in instruments if i.get("name") == "NIFTY" and i.get("instrument_type") in ("CE", "PE")][:3]
    for i in nifty_opts:
        print(f"  {i['tradingsymbol']:30s}  strike={i['strike']:>8}  exp={i['expiry']}  token={i['instrument_token']}")

    print(f"\n[OK] Login + API calls work. Total F&O instruments: {len(instruments)}")
    print(f"\nSave this access_token if you want to test more:\n{access_token}")


if __name__ == "__main__":
    main()
