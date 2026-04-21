"""
Kite Connect options-chain fetcher.

Returns the same summary shape as options_chain.summarize_chain() so the
strategy engine + paper-trade storage need no changes.

Kite does NOT return implied volatility directly in quote() — we compute IV
with a Black-Scholes Newton-Raphson inverse (pure math, no scipy).

Usage requires two env vars:
  KITE_API_KEY
  KITE_ACCESS_TOKEN   (refreshed daily via kite_refresh_token.py)
"""

import os
import math
import asyncio
from datetime import date, datetime
from typing import Optional
from loguru import logger

try:
    from kiteconnect import KiteConnect
except ImportError:  # Kite is optional; failure only bites when enabled
    KiteConnect = None  # type: ignore


RISK_FREE_RATE = 0.0675  # India 10Y G-Sec approximation; good enough for regime classification


# Underlying spot instrument lookup. Kite spot tickers differ from F&O names.
SPOT_INSTRUMENT = {
    "NIFTY": "NSE:NIFTY 50",
    "BANKNIFTY": "NSE:NIFTY BANK",
}


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    return math.exp(-x * x / 2.0) / math.sqrt(2.0 * math.pi)


def _bs_price(S: float, K: float, T: float, r: float, sigma: float, is_call: bool) -> float:
    if T <= 0 or sigma <= 0:
        return max(0.0, (S - K) if is_call else (K - S))
    d1 = (math.log(S / K) + (r + sigma * sigma / 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if is_call:
        return S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
    return K * math.exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)


def _bs_vega(S: float, K: float, T: float, r: float, sigma: float) -> float:
    if T <= 0 or sigma <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + sigma * sigma / 2) * T) / (sigma * math.sqrt(T))
    return S * _norm_pdf(d1) * math.sqrt(T)


def implied_volatility(market_price: float, S: float, K: float, T: float,
                       is_call: bool, r: float = RISK_FREE_RATE) -> float:
    """Newton-Raphson inverse of Black-Scholes. Returns IV as a percentage (e.g. 15.2).
    Returns 0 if inputs are degenerate or it fails to converge."""
    if market_price <= 0 or S <= 0 or K <= 0 or T <= 0:
        return 0.0
    # Discard options that are deep ITM and priced near intrinsic — BS IV is unstable there.
    intrinsic = max(0.0, (S - K) if is_call else (K - S))
    if market_price <= intrinsic + 0.05:
        return 0.0

    sigma = 0.25  # seed
    for _ in range(40):
        price = _bs_price(S, K, T, r, sigma, is_call)
        diff = price - market_price
        if abs(diff) < 0.01:
            return round(sigma * 100, 2)
        vega = _bs_vega(S, K, T, r, sigma)
        if vega < 1e-6:
            break
        sigma -= diff / vega
        if sigma <= 0.001:
            sigma = 0.001
        if sigma > 5:
            sigma = 5
    return round(sigma * 100, 2) if 0 < sigma < 5 else 0.0


class KiteOptionsChainFetcher:
    """Drop-in replacement for options_chain.OptionsChainFetcher when Kite is configured.

    Exposes fetch_chain_summary(symbol, is_index, expiry=None) returning the same dict
    shape as options_chain.summarize_chain() so the strategy + storage layers are untouched.
    """

    def __init__(self):
        if KiteConnect is None:
            raise RuntimeError("kiteconnect SDK not installed. Run: pip install kiteconnect")
        api_key = os.environ.get("KITE_API_KEY", "").strip()
        access_token = os.environ.get("KITE_ACCESS_TOKEN", "").strip()
        if not api_key or not access_token:
            raise RuntimeError("KITE_API_KEY or KITE_ACCESS_TOKEN not set")
        self._kite = KiteConnect(api_key=api_key)
        self._kite.set_access_token(access_token)
        self._instruments_cache: Optional[list] = None
        self._instruments_cache_date: Optional[date] = None

    def _instruments(self) -> list:
        today = date.today()
        if self._instruments_cache is None or self._instruments_cache_date != today:
            logger.info("Kite: fetching NFO instruments dump (cached for the day)")
            self._instruments_cache = self._kite.instruments("NFO")
            self._instruments_cache_date = today
            logger.info(f"Kite: loaded {len(self._instruments_cache)} NFO instruments")
        return self._instruments_cache

    def _spot_price(self, symbol: str) -> Optional[float]:
        ticker = SPOT_INSTRUMENT.get(symbol, f"NSE:{symbol}")
        try:
            data = self._kite.ltp([ticker])
            entry = data.get(ticker)
            if not entry:
                return None
            return float(entry.get("last_price") or 0) or None
        except Exception as e:
            logger.debug(f"Kite LTP failed for {ticker}: {e}")
            return None

    async def fetch_chain_summary(self, symbol: str, is_index: bool, expiry: Optional[str] = None) -> Optional[dict]:
        """Fetch + build chain summary. Returns None if data is unavailable.
        `expiry` format is NSE-style 'DD-Mon-YYYY' (e.g. '11-Jul-2024') to match the
        NSE fetcher's signature."""
        # Offload blocking Kite REST calls to a thread so we don't block the event loop.
        return await asyncio.to_thread(self._fetch_sync, symbol, is_index, expiry)

    def _fetch_sync(self, symbol: str, is_index: bool, expiry_str: Optional[str]) -> Optional[dict]:
        instruments = self._instruments()

        # Filter for this underlying's options (instrument_type must be CE or PE,
        # `name` matches underlying for both stocks and indices in Kite's dump).
        symbol_opts = [
            i for i in instruments
            if i.get("name") == symbol and i.get("instrument_type") in ("CE", "PE")
        ]
        if not symbol_opts:
            logger.debug(f"Kite: no F&O instruments for {symbol}")
            return None

        # Sorted list of unique expiry dates (as date objects).
        expiries = sorted({i["expiry"] for i in symbol_opts})
        if not expiries:
            return None

        if expiry_str:
            try:
                target_expiry = datetime.strptime(expiry_str, "%d-%b-%Y").date()
            except ValueError:
                target_expiry = expiries[0]
        else:
            target_expiry = expiries[0]

        expiry_opts = [i for i in symbol_opts if i["expiry"] == target_expiry]
        if not expiry_opts:
            logger.debug(f"Kite: no options for {symbol} on expiry {target_expiry}")
            return None

        # Spot
        spot = self._spot_price(symbol)
        if not spot:
            logger.debug(f"Kite: no spot price for {symbol}")
            return None

        # Batch quote — Kite allows up to 500 instruments per call. Typical chain has 80-100.
        tokens = [f"NFO:{i['tradingsymbol']}" for i in expiry_opts]
        try:
            quotes = self._kite.quote(tokens)
        except Exception as e:
            logger.debug(f"Kite quote failed for {symbol}: {e}")
            return None

        # Group per strike.
        strikes: dict = {}
        for inst in expiry_opts:
            key = f"NFO:{inst['tradingsymbol']}"
            q = quotes.get(key, {}) or {}
            strike = inst["strike"]
            if strike not in strikes:
                strikes[strike] = {
                    "strike": strike,
                    "ce_ltp": 0, "ce_oi": 0, "ce_iv": 0, "ce_volume": 0,
                    "pe_ltp": 0, "pe_oi": 0, "pe_iv": 0, "pe_volume": 0,
                }
            prefix = "ce" if inst["instrument_type"] == "CE" else "pe"
            strikes[strike][f"{prefix}_ltp"] = float(q.get("last_price") or 0)
            strikes[strike][f"{prefix}_oi"] = int(q.get("oi") or 0)
            strikes[strike][f"{prefix}_volume"] = int(q.get("volume") or 0)

        # Time to expiry in years (add half-day so same-day expiries don't divide by zero).
        days = max((target_expiry - date.today()).days, 0) + 0.5
        T = days / 365.0

        for row in strikes.values():
            K = row["strike"]
            if row["ce_ltp"] > 0:
                row["ce_iv"] = implied_volatility(row["ce_ltp"], spot, K, T, is_call=True)
            if row["pe_ltp"] > 0:
                row["pe_iv"] = implied_volatility(row["pe_ltp"], spot, K, T, is_call=False)

        strikes_list = sorted(strikes.values(), key=lambda s: s["strike"])
        atm = min(strikes_list, key=lambda s: abs(s["strike"] - spot))
        atm_iv = atm["ce_iv"] or atm["pe_iv"] or 0

        return {
            "spot": float(spot),
            "expiry": target_expiry.strftime("%d-%b-%Y"),
            "expiries": [e.strftime("%d-%b-%Y") for e in expiries[:6]],
            "atm_strike": atm["strike"],
            "atm_iv": float(atm_iv),
            "strikes": strikes_list,
        }

    async def close(self):
        # KiteConnect SDK uses sync requests — nothing to close.
        return


def kite_enabled() -> bool:
    """True if both Kite env vars are present."""
    return bool(os.environ.get("KITE_API_KEY", "").strip() and
                os.environ.get("KITE_ACCESS_TOKEN", "").strip())
