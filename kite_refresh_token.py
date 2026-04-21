"""
Daily Kite access_token refresher.

Run this each morning (takes ~10 seconds):

    set KITE_API_KEY=xxx
    set KITE_API_SECRET=xxx
    python kite_refresh_token.py

Flow:
  1. Script opens the Kite login URL in your browser
  2. You log in (Kite password + TOTP from your authenticator app)
  3. Kite redirects to your app's redirect_uri with ?request_token=XXX in the URL
  4. You copy that request_token from the browser URL bar and paste it here
  5. Script exchanges it for an access_token
  6. Paste the printed access_token into Railway env var KITE_ACCESS_TOKEN
     (Railway dashboard -> your service -> Variables -> edit)

The access_token is valid until ~6 AM IST the next day.
"""

import os
import sys
import webbrowser

try:
    from kiteconnect import KiteConnect
except ImportError:
    print("Missing kiteconnect. Run: python -m pip install kiteconnect")
    sys.exit(1)


API_KEY = os.environ.get("KITE_API_KEY", "").strip()
API_SECRET = os.environ.get("KITE_API_SECRET", "").strip()

if not API_KEY or not API_SECRET:
    print("ERROR: set KITE_API_KEY and KITE_API_SECRET env vars first.")
    sys.exit(1)


def main():
    kite = KiteConnect(api_key=API_KEY)
    login_url = kite.login_url()

    print(f"\nOpening login URL in browser:\n  {login_url}\n")
    try:
        webbrowser.open(login_url)
    except Exception:
        pass

    print("After logging in, Kite redirects to your app's redirect URL.")
    print("The redirect URL has ?request_token=XXX&action=login&status=success in it.")
    print("Copy the request_token value (the XXX part) and paste it below.\n")

    request_token = input("request_token: ").strip()
    if not request_token:
        print("No request_token provided, aborting.")
        sys.exit(1)

    print("\nExchanging request_token for access_token...")
    try:
        data = kite.generate_session(request_token, api_secret=API_SECRET)
    except Exception as e:
        print(f"ERROR: {e}")
        print("Common causes: request_token already used (can only exchange once), "
              "wrong API_SECRET, or request_token expired (valid for a few minutes only).")
        sys.exit(1)

    access_token = data["access_token"]
    user_id = data.get("user_id", "?")
    login_time = data.get("login_time", "?")

    print("\n" + "=" * 60)
    print(f"  user_id     : {user_id}")
    print(f"  login_time  : {login_time}")
    print(f"  access_token: {access_token}")
    print("=" * 60)
    print("\nNext steps:")
    print("  1. Copy the access_token above")
    print("  2. Open Railway dashboard -> your nse-stock-bot service -> Variables")
    print("  3. Set (or update) KITE_ACCESS_TOKEN to this value")
    print("  4. Railway will auto-redeploy; bot will use Kite for F&O chains")
    print("\nOr via Railway CLI:")
    print(f"  railway variables set KITE_ACCESS_TOKEN={access_token}")
    print()


if __name__ == "__main__":
    main()
