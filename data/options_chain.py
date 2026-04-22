"""
NSE Options Chain fetcher + analytics (IV regime, PCR, max pain, ATM).

Direct NSE path kept as fallback; Kite Connect (kite_client.py) is the primary
backend for Railway deployments since NSE geo-blocks cloud IPs.

LOT_SIZES reflect NSE's Dec-2024 revision (Nifty 25->75, BankNifty 15->30, etc.).
"""

import time
import httpx
from typing import Optional
from loguru import logger


INDICES = ["NIFTY", "BANKNIFTY"]

FNO_STOCKS = [
    "RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS",
    "SBIN", "AXISBANK", "KOTAKBANK", "ITC", "LT",
    "HINDUNILVR", "BHARTIARTL", "MARUTI", "TATAMOTORS", "BAJFINANCE",
    "ADANIENT", "WIPRO", "HCLTECH", "SUNPHARMA", "ASIANPAINT",
]

# NSE F&O lot sizes (revised post Dec-2024). Values for first 10 symbols were
# provided by user directly; remainder approximated from public sources and
# should be verified before real-money trading.
LOT_SIZES = {
    "NIFTY": 75, "BANKNIFTY": 30,
    "RELIANCE": 250, "TCS": 150, "INFY": 300, "HDFCBANK": 550,
    "ICICIBANK": 700, "WIPRO": 3000, "AXISBANK": 1200, "KOTAKBANK": 400,
    "SBIN": 1500, "ITC": 1600, "LT": 300, "HINDUNILVR": 300,
    "BHARTIARTL": 475, "MARUTI": 50, "TATAMOTORS": 1425, "BAJFINANCE": 125,
    "ADANIENT": 300, "HCLTECH": 350, "SUNPHARMA": 700, "ASIANPAINT": 200,
}


class OptionsChainFetcher:
    """Direct-NSE option-chain fetcher. Works locally; blocked on Railway/GCP (403)."""

    INDICES_URL = "https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
    EQUITY_URL = "https://www.nseindia.com/api/option-chain-equities?symbol={symbol}"
    NSE_BASE_URL = "https://www.nseindia.com"

    def __init__(self):
        self._cookies = None
        self._cookie_time = None
        self._blocked = False
        self._client = httpx.AsyncClient(
            timeout=20,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            },
            follow_redirects=True,
        )

    async def _refresh_cookies(self) -> bool:
        if self._cookies and self._cookie_time and (time.time() - self._cookie_time < 300):
            return True
        try:
            resp = await self._client.get(
                self.NSE_BASE_URL,
                headers={"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
            )
            if resp.status_code == 200:
                self._cookies = dict(resp.cookies)
                self._cookie_time = time.time()
                self._blocked = False
                return True
            self._blocked = True
            return False
        except Exception as e:
            logger.debug(f"Options cookie refresh failed: {e}")
            self._blocked = True
            return False

    async def fetch_chain(self, symbol: str, is_index: bool) -> Optional[dict]:
        if self._blocked:
            return None
        if not await self._refresh_cookies():
            return None

        url = (self.INDICES_URL if is_index else self.EQUITY_URL).format(symbol=symbol)
        referer = (
            "https://www.nseindia.com/option-chain"
            if is_index
            else f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}"
        )
        try:
            resp = await self._client.get(
                url,
                headers={"Referer": referer, "Accept": "application/json, text/plain, */*"},
                cookies=self._cookies,
            )
            if resp.status_code == 403:
                logger.info(f"NSE options 403 — IP blocked. {symbol} chain unavailable.")
                self._blocked = True
                return None
            if resp.status_code != 200:
                logger.debug(f"NSE options {resp.status_code} for {symbol}")
                return None
            return resp.json()
        except Exception as e:
            logger.debug(f"Options chain fetch failed for {symbol}: {e}")
            return None

    async def fetch_chain_summary(self, symbol: str, is_index: bool, expiry: Optional[str] = None) -> Optional[dict]:
        """Unified interface matching KiteOptionsChainFetcher.fetch_chain_summary."""
        raw = await self.fetch_chain(symbol, is_index)
        if not raw:
            return None
        return summarize_chain(raw, expiry)

    async def close(self):
        await self._client.aclose()


def summarize_chain(raw: dict, expiry: Optional[str] = None) -> Optional[dict]:
    """Structured summary of a raw NSE chain JSON."""
    records = raw.get("records", {}) if raw else {}
    data = records.get("data", [])
    expiries = records.get("expiryDates", [])
    spot = records.get("underlyingValue")
    if not data or not expiries or not spot:
        return None

    chosen = expiry or expiries[0]
    rows = [d for d in data if d.get("expiryDate") == chosen]
    if not rows:
        return None

    strikes = []
    for row in rows:
        ce = row.get("CE") or {}
        pe = row.get("PE") or {}
        strikes.append({
            "strike": row.get("strikePrice"),
            "ce_ltp": ce.get("lastPrice") or 0,
            "ce_oi": ce.get("openInterest") or 0,
            "ce_iv": ce.get("impliedVolatility") or 0,
            "ce_volume": ce.get("totalTradedVolume") or 0,
            "pe_ltp": pe.get("lastPrice") or 0,
            "pe_oi": pe.get("openInterest") or 0,
            "pe_iv": pe.get("impliedVolatility") or 0,
            "pe_volume": pe.get("totalTradedVolume") or 0,
        })
    strikes.sort(key=lambda s: s["strike"])
    atm = min(strikes, key=lambda s: abs(s["strike"] - spot))
    atm_iv = atm["ce_iv"] or atm["pe_iv"] or 0
    return {
        "spot": float(spot),
        "expiry": chosen,
        "expiries": expiries[:6],
        "atm_strike": atm["strike"],
        "atm_iv": float(atm_iv),
        "strikes": strikes,
    }


def compute_pcr(summary: dict) -> float:
    ce_oi = sum(s["ce_oi"] for s in summary["strikes"])
    pe_oi = sum(s["pe_oi"] for s in summary["strikes"])
    return round(pe_oi / ce_oi, 2) if ce_oi else 0.0


def compute_max_pain(summary: dict) -> int:
    strikes = summary["strikes"]
    best_strike = strikes[0]["strike"]
    best_loss = float("inf")
    for candidate in strikes:
        k = candidate["strike"]
        loss = 0.0
        for s in strikes:
            loss += max(0, k - s["strike"]) * s["ce_oi"]
            loss += max(0, s["strike"] - k) * s["pe_oi"]
        if loss < best_loss:
            best_loss = loss
            best_strike = k
    return best_strike


def iv_regime(atm_iv: float, is_index: bool) -> str:
    if is_index:
        if atm_iv < 13:
            return "low"
        if atm_iv > 20:
            return "high"
        return "neutral"
    if atm_iv < 22:
        return "low"
    if atm_iv > 38:
        return "high"
    return "neutral"


def iv_rank_in_chain(summary: dict) -> float:
    ivs = [s["ce_iv"] for s in summary["strikes"] if s["ce_iv"] > 0]
    ivs += [s["pe_iv"] for s in summary["strikes"] if s["pe_iv"] > 0]
    if not ivs:
        return 50.0
    atm_iv = summary["atm_iv"]
    below = sum(1 for v in ivs if v < atm_iv)
    return round(below / len(ivs) * 100, 1)


def pick_strike(summary: dict, offset_steps: int) -> Optional[dict]:
    strikes = summary["strikes"]
    atm_idx = next((i for i, s in enumerate(strikes) if s["strike"] == summary["atm_strike"]), None)
    if atm_idx is None:
        return None
    target = atm_idx + offset_steps
    if target < 0 or target >= len(strikes):
        return None
    return strikes[target]


def nearest_strike(summary: dict, target_price: float) -> Optional[dict]:
    """Snap a requested strike price to the nearest listed strike (for agent outputs)."""
    if not summary.get("strikes"):
        return None
    return min(summary["strikes"], key=lambda s: abs(s["strike"] - target_price))
