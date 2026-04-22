"""Global market cues for F&O reasoning context.

Fetches: India VIX, SGX/GIFT Nifty (via Yahoo ^NSEI proxy), US futures (ES=F, NQ=F),
crude oil (CL=F). Returns a single dict for the research agent to summarize.

All data via Yahoo Finance chart API (no auth) to keep it portable — Kite for
India VIX would require an LTP call which is fine but Yahoo works both locally
and on Railway without token refresh.
"""

import time
import httpx
from typing import Optional
from loguru import logger


YAHOO_QUOTE = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=2d"

CUES = {
    "india_vix": "^INDIAVIX",
    "nifty": "^NSEI",
    "sensex": "^BSESN",
    "us_sp500_fut": "ES=F",
    "us_nasdaq_fut": "NQ=F",
    "dow_fut": "YM=F",
    "crude_oil": "CL=F",
    "dxy": "DX-Y.NYB",
    "gold": "GC=F",
}


_CACHE: dict = {"ts": 0.0, "data": None}
_TTL_SECONDS = 300  # cache global cues for 5 min to avoid re-fetching per symbol


async def fetch_global_cues() -> dict:
    """Returns {cue_name: {last, change, change_pct}} for all tickers. Cached 5 min."""
    now = time.time()
    if _CACHE["data"] is not None and now - _CACHE["ts"] < _TTL_SECONDS:
        return _CACHE["data"]

    async with httpx.AsyncClient(timeout=15, headers={"User-Agent": "Mozilla/5.0"}) as client:
        out: dict = {}
        for key, ticker in CUES.items():
            out[key] = await _fetch_one(client, ticker)
    _CACHE["data"] = out
    _CACHE["ts"] = now
    return out


async def _fetch_one(client: httpx.AsyncClient, ticker: str) -> Optional[dict]:
    try:
        resp = await client.get(YAHOO_QUOTE.format(symbol=ticker))
        if resp.status_code != 200:
            return None
        result = resp.json().get("chart", {}).get("result", [])
        if not result:
            return None
        meta = result[0].get("meta", {})
        last = meta.get("regularMarketPrice") or meta.get("previousClose")
        prev = meta.get("chartPreviousClose") or meta.get("previousClose")
        if last is None:
            return None
        change = (last - prev) if prev else 0
        change_pct = (change / prev * 100) if prev else 0
        return {
            "last": round(float(last), 2),
            "change": round(float(change), 2),
            "change_pct": round(float(change_pct), 2),
        }
    except Exception as e:
        logger.debug(f"Global cue fetch failed for {ticker}: {e}")
        return None


def format_cues_for_prompt(cues: dict) -> str:
    """One-line-per-cue formatting for LLM prompts."""
    lines = []
    for key, val in cues.items():
        if val is None:
            lines.append(f"{key}: unavailable")
            continue
        lines.append(f"{key}: {val['last']:.2f} ({val['change_pct']:+.2f}%)")
    return "\n".join(lines)
