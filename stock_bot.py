"""
NSE/BSE Stock Trading Bot — Phase 1 (Paper Trading)
Claude-powered AI reasoning for Indian stock markets.

Covers: Swing, Intraday, Positional trading
Signals: Buy/Sell + Entry + Stop Loss + Target
Analysis: Technical (RSI, MACD, EMA) + Fundamental (PE, earnings)
"""

import os
import json
import math
import asyncio
import aiosqlite
import httpx
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional
from loguru import logger
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from pydantic import ConfigDict
from pathlib import Path


# ── Settings ──────────────────────────────────────────────────────────────────
class Settings(BaseSettings):
    DEEPSEEK_API_KEY: str = ""
    DEEPSEEK_BASE_URL: str = "https://api.deepseek.com"
    DEEPSEEK_MODEL: str = "deepseek-chat"
    GEMINI_API_KEY: str = ""  # kept for fallback / migration, not used
    PHASE: int = 1
    PAPER_TRADING: bool = True
    LIVE_TRADING_ENABLED: bool = False
    STARTING_CAPITAL: float = 100000.0  # Rs 1 lakh default
    DB_PATH: str = "/app/data/stock_bot.db" if os.environ.get("RAILWAY_ENVIRONMENT") else "data/stock_bot.db"
    SCAN_INTERVAL_MINUTES: int = 60
    MIN_CONFIDENCE: float = 0.55  # lowered from 0.65 — DeepSeek produces lower confidence than Gemini
    MAX_POSITION_PCT: float = 0.05  # 5% per stock
    TRADING_MODES: list = ["swing", "intraday", "positional"]

    model_config = ConfigDict(env_file=".env", extra="allow")

settings = Settings()


# ── Models ────────────────────────────────────────────────────────────────────
class StockSignal(BaseModel):
    symbol: str
    company_name: str
    exchange: str = "NSE"
    signal: str  # BUY / SELL / HOLD
    strength: str  # STRONG / MODERATE / WEAK
    trading_mode: str  # swing / intraday / positional
    current_price: float
    entry_price: float
    stop_loss: float
    target_1: float
    target_2: float
    target_3: float
    risk_reward: float
    position_size_pct: float
    position_size_inr: float
    confidence: float
    reasoning: str
    technical_summary: str
    fundamental_summary: str
    rsi: Optional[float] = None
    macd_signal: Optional[str] = None
    trend: Optional[str] = None
    sector: Optional[str] = None
    pe_ratio: Optional[float] = None
    timestamp: datetime = datetime.now(tz=None)


# ── Stock Universe — Top NSE stocks across sectors ────────────────────────────
NSE_WATCHLIST = [
    # ── Nifty 50 ─────────────────────────────────────────────────────────────
    {"symbol": "RELIANCE", "name": "Reliance Industries", "sector": "Energy", "market_cap": "large"},
    {"symbol": "TCS", "name": "Tata Consultancy Services", "sector": "IT", "market_cap": "large"},
    {"symbol": "HDFCBANK", "name": "HDFC Bank", "sector": "Banking", "market_cap": "large"},
    {"symbol": "INFY", "name": "Infosys", "sector": "IT", "market_cap": "large"},
    {"symbol": "ICICIBANK", "name": "ICICI Bank", "sector": "Banking", "market_cap": "large"},
    {"symbol": "HINDUNILVR", "name": "Hindustan Unilever", "sector": "FMCG", "market_cap": "large"},
    {"symbol": "SBIN", "name": "State Bank of India", "sector": "Banking", "market_cap": "large"},
    {"symbol": "BAJFINANCE", "name": "Bajaj Finance", "sector": "NBFC", "market_cap": "large"},
    {"symbol": "BHARTIARTL", "name": "Bharti Airtel", "sector": "Telecom", "market_cap": "large"},
    {"symbol": "KOTAKBANK", "name": "Kotak Mahindra Bank", "sector": "Banking", "market_cap": "large"},
    {"symbol": "LT", "name": "Larsen & Toubro", "sector": "Infrastructure", "market_cap": "large"},
    {"symbol": "AXISBANK", "name": "Axis Bank", "sector": "Banking", "market_cap": "large"},
    {"symbol": "ASIANPAINT", "name": "Asian Paints", "sector": "Paints", "market_cap": "large"},
    {"symbol": "MARUTI", "name": "Maruti Suzuki", "sector": "Auto", "market_cap": "large"},
    {"symbol": "TITAN", "name": "Titan Company", "sector": "Consumer", "market_cap": "large"},
    {"symbol": "ITC", "name": "ITC Limited", "sector": "FMCG", "market_cap": "large"},
    {"symbol": "SUNPHARMA", "name": "Sun Pharma", "sector": "Pharma", "market_cap": "large"},
    {"symbol": "WIPRO", "name": "Wipro", "sector": "IT", "market_cap": "large"},
    {"symbol": "HCLTECH", "name": "HCL Technologies", "sector": "IT", "market_cap": "large"},
    {"symbol": "TECHM", "name": "Tech Mahindra", "sector": "IT", "market_cap": "large"},
    {"symbol": "NTPC", "name": "NTPC Limited", "sector": "Power", "market_cap": "large"},
    {"symbol": "POWERGRID", "name": "Power Grid Corp", "sector": "Power", "market_cap": "large"},
    {"symbol": "ONGC", "name": "Oil & Natural Gas Corp", "sector": "Energy", "market_cap": "large"},
    {"symbol": "TATAMOTORS", "name": "Tata Motors", "sector": "Auto", "market_cap": "large"},
    {"symbol": "TATASTEEL", "name": "Tata Steel", "sector": "Metals", "market_cap": "large"},
    {"symbol": "ULTRACEMCO", "name": "UltraTech Cement", "sector": "Cement", "market_cap": "large"},
    {"symbol": "ADANIENT", "name": "Adani Enterprises", "sector": "Conglomerate", "market_cap": "large"},
    {"symbol": "ADANIPORTS", "name": "Adani Ports", "sector": "Infrastructure", "market_cap": "large"},
    {"symbol": "BAJAJFINSV", "name": "Bajaj Finserv", "sector": "NBFC", "market_cap": "large"},
    {"symbol": "BAJAJ-AUTO", "name": "Bajaj Auto", "sector": "Auto", "market_cap": "large"},
    {"symbol": "JSWSTEEL", "name": "JSW Steel", "sector": "Metals", "market_cap": "large"},
    {"symbol": "M&M", "name": "Mahindra & Mahindra", "sector": "Auto", "market_cap": "large"},
    {"symbol": "NESTLEIND", "name": "Nestle India", "sector": "FMCG", "market_cap": "large"},
    {"symbol": "DRREDDY", "name": "Dr Reddys Labs", "sector": "Pharma", "market_cap": "large"},
    {"symbol": "CIPLA", "name": "Cipla", "sector": "Pharma", "market_cap": "large"},
    {"symbol": "COALINDIA", "name": "Coal India", "sector": "Mining", "market_cap": "large"},
    {"symbol": "GRASIM", "name": "Grasim Industries", "sector": "Cement", "market_cap": "large"},
    {"symbol": "BPCL", "name": "Bharat Petroleum", "sector": "Energy", "market_cap": "large"},
    {"symbol": "DIVISLAB", "name": "Divis Laboratories", "sector": "Pharma", "market_cap": "large"},
    {"symbol": "BRITANNIA", "name": "Britannia Industries", "sector": "FMCG", "market_cap": "large"},
    {"symbol": "EICHERMOT", "name": "Eicher Motors", "sector": "Auto", "market_cap": "large"},
    {"symbol": "APOLLOHOSP", "name": "Apollo Hospitals", "sector": "Healthcare", "market_cap": "large"},
    {"symbol": "HEROMOTOCO", "name": "Hero MotoCorp", "sector": "Auto", "market_cap": "large"},
    {"symbol": "INDUSINDBK", "name": "IndusInd Bank", "sector": "Banking", "market_cap": "large"},
    {"symbol": "HINDALCO", "name": "Hindalco Industries", "sector": "Metals", "market_cap": "large"},
    {"symbol": "SHRIRAMFIN", "name": "Shriram Finance", "sector": "NBFC", "market_cap": "large"},
    {"symbol": "SBILIFE", "name": "SBI Life Insurance", "sector": "Insurance", "market_cap": "large"},
    {"symbol": "HDFCLIFE", "name": "HDFC Life Insurance", "sector": "Insurance", "market_cap": "large"},
    {"symbol": "TATACONSUM", "name": "Tata Consumer Products", "sector": "FMCG", "market_cap": "large"},
    # ── Nifty Next 50 / Large-Mid ────────────────────────────────────────────
    {"symbol": "BANKBARODA", "name": "Bank of Baroda", "sector": "Banking", "market_cap": "large"},
    {"symbol": "PNB", "name": "Punjab National Bank", "sector": "Banking", "market_cap": "large"},
    {"symbol": "CANBK", "name": "Canara Bank", "sector": "Banking", "market_cap": "large"},
    {"symbol": "IDFCFIRSTB", "name": "IDFC First Bank", "sector": "Banking", "market_cap": "large"},
    {"symbol": "FEDERALBNK", "name": "Federal Bank", "sector": "Banking", "market_cap": "mid"},
    {"symbol": "VEDL", "name": "Vedanta Limited", "sector": "Metals", "market_cap": "large"},
    {"symbol": "GODREJCP", "name": "Godrej Consumer", "sector": "FMCG", "market_cap": "large"},
    {"symbol": "DABUR", "name": "Dabur India", "sector": "FMCG", "market_cap": "large"},
    {"symbol": "PIDILITIND", "name": "Pidilite Industries", "sector": "Chemicals", "market_cap": "large"},
    {"symbol": "HAVELLS", "name": "Havells India", "sector": "Electricals", "market_cap": "large"},
    {"symbol": "SIEMENS", "name": "Siemens", "sector": "Capital Goods", "market_cap": "large"},
    {"symbol": "ABB", "name": "ABB India", "sector": "Capital Goods", "market_cap": "large"},
    {"symbol": "BOSCHLTD", "name": "Bosch Limited", "sector": "Auto Ancillary", "market_cap": "large"},
    {"symbol": "AMBUJACEM", "name": "Ambuja Cements", "sector": "Cement", "market_cap": "large"},
    {"symbol": "SHREECEM", "name": "Shree Cement", "sector": "Cement", "market_cap": "large"},
    {"symbol": "DLF", "name": "DLF Limited", "sector": "Real Estate", "market_cap": "large"},
    {"symbol": "GODREJPROP", "name": "Godrej Properties", "sector": "Real Estate", "market_cap": "large"},
    {"symbol": "OBEROIRLTY", "name": "Oberoi Realty", "sector": "Real Estate", "market_cap": "mid"},
    {"symbol": "HAL", "name": "Hindustan Aeronautics", "sector": "Defence", "market_cap": "large"},
    {"symbol": "BEL", "name": "Bharat Electronics", "sector": "Defence", "market_cap": "large"},
    {"symbol": "BHEL", "name": "Bharat Heavy Electricals", "sector": "Capital Goods", "market_cap": "large"},
    {"symbol": "IOC", "name": "Indian Oil Corp", "sector": "Energy", "market_cap": "large"},
    {"symbol": "GAIL", "name": "GAIL India", "sector": "Energy", "market_cap": "large"},
    {"symbol": "ADANIGREEN", "name": "Adani Green Energy", "sector": "Renewable Energy", "market_cap": "large"},
    {"symbol": "ADANIPOWER", "name": "Adani Power", "sector": "Power", "market_cap": "large"},
    {"symbol": "TATAPOWER", "name": "Tata Power", "sector": "Power", "market_cap": "large"},
    {"symbol": "NHPC", "name": "NHPC Limited", "sector": "Power", "market_cap": "large"},
    {"symbol": "RECLTD", "name": "REC Limited", "sector": "NBFC", "market_cap": "large"},
    {"symbol": "PFC", "name": "Power Finance Corp", "sector": "NBFC", "market_cap": "large"},
    {"symbol": "IRFC", "name": "Indian Railway Finance", "sector": "NBFC", "market_cap": "large"},
    {"symbol": "CHOLAFIN", "name": "Cholamandalam Finance", "sector": "NBFC", "market_cap": "large"},
    {"symbol": "MUTHOOTFIN", "name": "Muthoot Finance", "sector": "NBFC", "market_cap": "mid"},
    {"symbol": "ICICIPRULI", "name": "ICICI Pru Life", "sector": "Insurance", "market_cap": "large"},
    {"symbol": "LUPIN", "name": "Lupin Limited", "sector": "Pharma", "market_cap": "large"},
    {"symbol": "TORNTPHARM", "name": "Torrent Pharma", "sector": "Pharma", "market_cap": "large"},
    {"symbol": "ZYDUSLIFE", "name": "Zydus Lifesciences", "sector": "Pharma", "market_cap": "large"},
    {"symbol": "MAXHEALTH", "name": "Max Healthcare", "sector": "Healthcare", "market_cap": "large"},
    # ── Popular Mid Caps ─────────────────────────────────────────────────────
    {"symbol": "PERSISTENT", "name": "Persistent Systems", "sector": "IT", "market_cap": "mid"},
    {"symbol": "DIXON", "name": "Dixon Technologies", "sector": "Electronics", "market_cap": "mid"},
    {"symbol": "IRCTC", "name": "IRCTC", "sector": "Travel", "market_cap": "mid"},
    {"symbol": "POLYCAB", "name": "Polycab India", "sector": "Cables", "market_cap": "mid"},
    {"symbol": "TRENT", "name": "Trent Limited", "sector": "Retail", "market_cap": "mid"},
    {"symbol": "ZOMATO", "name": "Zomato", "sector": "Internet", "market_cap": "mid"},
    {"symbol": "PAYTM", "name": "One97 Communications", "sector": "Fintech", "market_cap": "mid"},
    {"symbol": "NYKAA", "name": "FSN E-Commerce", "sector": "Internet", "market_cap": "mid"},
    {"symbol": "DELHIVERY", "name": "Delhivery", "sector": "Logistics", "market_cap": "mid"},
    {"symbol": "SUPREMEIND", "name": "Supreme Industries", "sector": "Plastics", "market_cap": "mid"},
    {"symbol": "COFORGE", "name": "Coforge Limited", "sector": "IT", "market_cap": "mid"},
    {"symbol": "LTIM", "name": "LTIMindtree", "sector": "IT", "market_cap": "large"},
    {"symbol": "MPHASIS", "name": "Mphasis", "sector": "IT", "market_cap": "mid"},
    {"symbol": "DEEPAKNTR", "name": "Deepak Nitrite", "sector": "Chemicals", "market_cap": "mid"},
    {"symbol": "PIIND", "name": "PI Industries", "sector": "Chemicals", "market_cap": "mid"},
    {"symbol": "ASTRAL", "name": "Astral Limited", "sector": "Pipes", "market_cap": "mid"},
    {"symbol": "VOLTAS", "name": "Voltas Limited", "sector": "Consumer Durables", "market_cap": "mid"},
    {"symbol": "PAGEIND", "name": "Page Industries", "sector": "Textiles", "market_cap": "mid"},
    {"symbol": "CUMMINSIND", "name": "Cummins India", "sector": "Capital Goods", "market_cap": "mid"},
    {"symbol": "SUNDARMFIN", "name": "Sundaram Finance", "sector": "NBFC", "market_cap": "mid"},
    {"symbol": "INDIANB", "name": "Indian Bank", "sector": "Banking", "market_cap": "mid"},
    {"symbol": "AUBANK", "name": "AU Small Finance Bank", "sector": "Banking", "market_cap": "mid"},
    {"symbol": "PHOENIXLTD", "name": "Phoenix Mills", "sector": "Real Estate", "market_cap": "mid"},
    {"symbol": "PRESTIGE", "name": "Prestige Estates", "sector": "Real Estate", "market_cap": "mid"},
    {"symbol": "SOLARINDS", "name": "Solar Industries", "sector": "Defence", "market_cap": "mid"},
    {"symbol": "COCHINSHIP", "name": "Cochin Shipyard", "sector": "Defence", "market_cap": "mid"},
    {"symbol": "MAZAGON", "name": "Mazagon Dock", "sector": "Defence", "market_cap": "mid"},
    {"symbol": "JIOFIN", "name": "Jio Financial Services", "sector": "NBFC", "market_cap": "large"},
    {"symbol": "LICI", "name": "LIC of India", "sector": "Insurance", "market_cap": "large"},
]


# ── NSE India Live + yfinance Historical Data Fetcher ─────────────────────────
class StockDataFetcher:
    """Fetches live prices from NSE India API, historical data from yfinance."""

    NSE_QUOTE_URL = "https://www.nseindia.com/api/quote-equity?symbol={symbol}"
    NSE_BASE_URL = "https://www.nseindia.com"

    def __init__(self):
        self._nse_cookies = None
        self._nse_cookie_time = None
        self._nse_blocked = False  # If True, skip NSE and use Yahoo only
        self._http = httpx.AsyncClient(timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        self._nse_client = httpx.AsyncClient(
            timeout=15,
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

    async def _refresh_nse_cookies(self):
        """Visit NSE homepage to get session cookies. Required before API calls."""
        import time
        # Skip if cookies are fresh (< 5 min old)
        if self._nse_cookies and self._nse_cookie_time and (time.time() - self._nse_cookie_time < 300):
            return True
        try:
            resp = await self._nse_client.get(
                self.NSE_BASE_URL,
                headers={"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
            )
            if resp.status_code == 200:
                self._nse_cookies = dict(resp.cookies)
                self._nse_cookie_time = time.time()
                self._nse_blocked = False
                logger.debug(f"NSE cookies refreshed: {list(self._nse_cookies.keys())}")
                return True
            else:
                logger.debug(f"NSE homepage returned {resp.status_code} — likely IP-blocked")
                self._nse_blocked = True
                return False
        except Exception as e:
            logger.debug(f"NSE cookie refresh failed: {e}")
            self._nse_blocked = True
            return False

    async def _fetch_nse_live(self, symbol: str) -> Optional[dict]:
        """Get real-time quote from NSE India API."""
        # Skip NSE entirely if we know it's blocked (cloud IP)
        if self._nse_blocked:
            return None
        try:
            # Ensure we have valid session cookies
            await self._refresh_nse_cookies()
            if self._nse_blocked:
                return None

            referer = f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}"
            url = self.NSE_QUOTE_URL.format(symbol=symbol)
            resp = await self._nse_client.get(
                url,
                headers={
                    "Referer": referer,
                    "Accept": "application/json, text/plain, */*",
                },
                cookies=self._nse_cookies,
            )

            if resp.status_code == 403:
                logger.info(f"NSE API 403 — IP blocked by NSE. Switching to Yahoo-only mode.")
                self._nse_blocked = True
                return None
            if resp.status_code != 200:
                logger.debug(f"NSE API {resp.status_code} for {symbol}")
                return None

            data = resp.json()
            price_info = data.get("priceInfo", {})
            metadata = data.get("metadata", {})

            current_price = price_info.get("lastPrice")
            if not current_price:
                return None

            # Fetch volume from trade_info endpoint
            volume = 0
            try:
                trade_resp = await self._nse_client.get(
                    f"{url}&section=trade_info", headers={"Referer": referer}
                )
                if trade_resp.status_code == 200:
                    trade_data = trade_resp.json()
                    volume = int(trade_data.get("securityWiseDP", {}).get("quantityTraded", 0) or 0)
            except Exception:
                pass

            return {
                "nse_live": True,
                "current_price": float(current_price),
                "prev_close": float(price_info.get("previousClose", 0)),
                "open": float(price_info.get("open", 0)),
                "day_high": float(price_info.get("intraDayHighLow", {}).get("max", 0)),
                "day_low": float(price_info.get("intraDayHighLow", {}).get("min", 0)),
                "volume": volume,
                "52w_high": float(price_info.get("weekHighLow", {}).get("max", 0)),
                "52w_low": float(price_info.get("weekHighLow", {}).get("min", 0)),
                "pe_ratio": float(metadata.get("pdSymbolPe", 0) or 0) or None,
                "change": float(price_info.get("change", 0)),
                "change_pct": float(price_info.get("pChange", 0)),
                "market_cap": float(metadata.get("marketCap", 0) or 0),
            }
        except Exception as e:
            logger.debug(f"NSE live fetch failed for {symbol}: {e}")
            return None

    async def _fetch_yf_history(self, symbol: str) -> Optional[dict]:
        """Get 3-month historical data from Yahoo Finance HTTP API (no pandas/numpy).
        Also extracts real-time quote data from the chart metadata."""
        yf_symbol = f"{symbol}.NS"
        try:
            resp = await self._http.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{yf_symbol}",
                params={"interval": "1d", "range": "3mo"},
            )
            if resp.status_code != 200:
                # Try BSE suffix as fallback
                resp = await self._http.get(
                    f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.BO",
                    params={"interval": "1d", "range": "3mo"},
                )
                if resp.status_code != 200:
                    return None
            data = resp.json()
            result = data.get("chart", {}).get("result", [])
            if not result:
                return None
            meta = result[0].get("meta", {})
            quote = result[0].get("indicators", {}).get("quote", [{}])[0]
            closes = [c for c in (quote.get("close") or []) if c is not None]
            highs = [h for h in (quote.get("high") or []) if h is not None]
            lows = [l for l in (quote.get("low") or []) if l is not None]
            volumes = [v for v in (quote.get("volume") or []) if v is not None]
            if not closes:
                return None
            # Extract real-time price from chart metadata (more current than last close)
            regular_price = meta.get("regularMarketPrice")
            prev_close = meta.get("chartPreviousClose") or meta.get("previousClose")
            return {
                "closes": closes[-60:],
                "highs": highs[-60:],
                "lows": lows[-60:],
                "volumes": volumes[-60:],
                "regular_market_price": regular_price,
                "chart_prev_close": prev_close,
                "fifty_two_week_high": meta.get("fiftyTwoWeekHigh"),
                "fifty_two_week_low": meta.get("fiftyTwoWeekLow"),
            }
        except Exception as e:
            logger.debug(f"Yahoo history failed for {symbol}: {e}")
            return None

    async def get_quote(self, symbol: str) -> Optional[dict]:
        """Get live price from NSE + historical data from Yahoo HTTP API."""
        # Step 1: Get historical data for technical analysis
        history = await self._fetch_yf_history(symbol)
        if not history or not history["closes"]:
            logger.warning(f"No historical data for {symbol}")
            return None

        # Step 2: Try NSE live price
        nse_data = await self._fetch_nse_live(symbol)

        if nse_data and nse_data.get("current_price"):
            logger.debug(f"{symbol}: NSE LIVE Rs {nse_data['current_price']:,.2f} ({nse_data.get('change_pct', 0):+.2f}%)")
            return {
                "symbol": symbol,
                "current_price": nse_data["current_price"],
                "prev_close": nse_data["prev_close"],
                "day_high": nse_data["day_high"],
                "day_low": nse_data["day_low"],
                "volume": nse_data["volume"],
                "52w_high": nse_data["52w_high"],
                "52w_low": nse_data["52w_low"],
                "market_cap": nse_data.get("market_cap", 0),
                "pe_ratio": nse_data.get("pe_ratio"),
                "closes": history["closes"],
                "highs": history["highs"],
                "lows": history["lows"],
                "volumes": history["volumes"],
                "source": "NSE_LIVE",
            }
        else:
            # Fallback: use Yahoo real-time price from chart metadata, or last close
            current_price = history.get("regular_market_price") or history["closes"][-1]
            prev_close = history.get("chart_prev_close") or (history["closes"][-2] if len(history["closes"]) > 1 else current_price)
            w52h = history.get("fifty_two_week_high") or (max(history["highs"]) if history["highs"] else current_price)
            w52l = history.get("fifty_two_week_low") or (min(history["lows"]) if history["lows"] else current_price)
            logger.debug(f"{symbol}: Yahoo fallback Rs {current_price:,.2f}")
            return {
                "symbol": symbol,
                "current_price": current_price,
                "prev_close": prev_close,
                "day_high": history["highs"][-1] if history["highs"] else current_price,
                "day_low": history["lows"][-1] if history["lows"] else current_price,
                "volume": history["volumes"][-1] if history["volumes"] else 0,
                "52w_high": w52h,
                "52w_low": w52l,
                "market_cap": 0,
                "pe_ratio": None,
                "closes": history["closes"],
                "highs": history["highs"],
                "lows": history["lows"],
                "volumes": history["volumes"],
                "source": "YAHOO_FALLBACK",
            }

    async def close(self):
        await self._nse_client.aclose()
        await self._http.aclose()


# ── Technical Analysis ────────────────────────────────────────────────────────
class TechnicalAnalyser:
    """Calculates key technical indicators."""

    @staticmethod
    def rsi(closes: list, period: int = 14) -> float:
        if len(closes) < period + 1:
            return 50.0
        deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
        gains = [d if d > 0 else 0 for d in deltas[-period:]]
        losses = [-d if d < 0 else 0 for d in deltas[-period:]]
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return round(100 - (100 / (1 + rs)), 2)

    @staticmethod
    def ema(closes: list, period: int) -> list:
        if len(closes) < period:
            return closes
        k = 2 / (period + 1)
        ema_vals = [sum(closes[:period]) / period]
        for price in closes[period:]:
            ema_vals.append(price * k + ema_vals[-1] * (1 - k))
        return ema_vals

    @staticmethod
    def macd(closes: list) -> dict:
        if len(closes) < 26:
            return {"macd": 0, "signal": 0, "histogram": 0, "crossover": "neutral"}
        ema12 = TechnicalAnalyser.ema(closes, 12)
        ema26 = TechnicalAnalyser.ema(closes, 26)
        min_len = min(len(ema12), len(ema26))
        macd_line = [ema12[-min_len+i] - ema26[-min_len+i] for i in range(min_len)]
        signal_line = TechnicalAnalyser.ema(macd_line, 9)
        histogram = macd_line[-1] - signal_line[-1] if signal_line else 0
        crossover = "bullish" if histogram > 0 else "bearish"
        return {
            "macd": round(macd_line[-1], 3),
            "signal": round(signal_line[-1] if signal_line else 0, 3),
            "histogram": round(histogram, 3),
            "crossover": crossover,
        }

    @staticmethod
    def support_resistance(highs: list, lows: list, closes: list) -> dict:
        if not highs or not lows or not closes:
            p = closes[-1] if closes else 0
            return {"support": p * 0.97, "resistance": p * 1.03, "pivot": p}
        recent_high = max(highs[-20:]) if len(highs) >= 20 else max(highs)
        recent_low = min(lows[-20:]) if len(lows) >= 20 else min(lows)
        pivot = (recent_high + recent_low + closes[-1]) / 3
        return {
            "support": round(recent_low, 2),
            "resistance": round(recent_high, 2),
            "pivot": round(pivot, 2),
        }

    @staticmethod
    def trend(closes: list) -> str:
        if len(closes) < 20:
            return "sideways"
        ema20 = TechnicalAnalyser.ema(closes, 20)
        ema50 = TechnicalAnalyser.ema(closes, min(50, len(closes)))
        if not ema20 or not ema50:
            return "sideways"
        if closes[-1] > ema20[-1] > ema50[-1]:
            return "uptrend"
        elif closes[-1] < ema20[-1] < ema50[-1]:
            return "downtrend"
        return "sideways"

    @staticmethod
    def volume_analysis(volumes: list) -> str:
        if len(volumes) < 10:
            return "normal"
        avg_vol = sum(volumes[-20:]) / min(20, len(volumes))
        current_vol = volumes[-1]
        if current_vol > avg_vol * 1.5:
            return "high"
        elif current_vol < avg_vol * 0.5:
            return "low"
        return "normal"

    @classmethod
    def full_analysis(cls, data: dict) -> dict:
        closes = data.get("closes", [])
        highs = data.get("highs", [])
        lows = data.get("lows", [])
        volumes = data.get("volumes", [])
        rsi_val = cls.rsi(closes)
        macd_data = cls.macd(closes)
        sr = cls.support_resistance(highs, lows, closes)
        trend_dir = cls.trend(closes)
        vol = cls.volume_analysis(volumes)
        current = data.get("current_price", closes[-1] if closes else 0)
        w52h = data.get("52w_high", current)
        w52l = data.get("52w_low", current)
        from_52h = round((current - w52h) / w52h * 100, 1) if w52h else 0
        from_52l = round((current - w52l) / w52l * 100, 1) if w52l else 0
        return {
            "rsi": rsi_val,
            "rsi_signal": "oversold" if rsi_val < 35 else "overbought" if rsi_val > 70 else "neutral",
            "macd": macd_data,
            "support": sr["support"],
            "resistance": sr["resistance"],
            "pivot": sr["pivot"],
            "trend": trend_dir,
            "volume": vol,
            "from_52w_high": from_52h,
            "from_52w_low": from_52l,
        }


# ── Claude Reasoning Engine ───────────────────────────────────────────────────
STOCK_REASONING_PROMPT = """You are an expert Indian stock market analyst with deep knowledge of NSE/BSE.
Analyse this stock and generate precise trading signals.

IMPORTANT — Signal balance:
You must generate a balanced mix of BUY, SELL, and HOLD signals based purely on the
technical data provided. If market data is insufficient to justify a strong directional
view, output HOLD. SELL signals are equally valid as BUY signals — do not default to BUY.
Overbought RSI + weak momentum is a SELL/HOLD setup, not a BUY.

## Stock Data
Symbol: {symbol} ({name}) | Sector: {sector}
Current Price: ₹{current_price}
Day Range: ₹{day_low} - ₹{day_high}
52W Range: ₹{w52l} - ₹{w52h}
Volume: {volume} ({volume_signal})
PE Ratio: {pe_ratio}

## Technical Analysis
RSI: {rsi} ({rsi_signal})
MACD: {macd_val} | Signal: {macd_signal} | Crossover: {macd_crossover}
Trend: {trend}
Support: ₹{support} | Resistance: ₹{resistance} | Pivot: ₹{pivot}
From 52W High: {from_52h}% | From 52W Low: {from_52l}%

## Market Context
Nifty50 trend: Base your view on the technical analysis below.
Sector: {sector}

## Recent News & Sentiment ({news_bias} bias from {news_total} articles)
{news_context}

Respond ONLY with valid JSON, no markdown:
{{
  "signal": "BUY" or "SELL" or "HOLD",
  "strength": "STRONG" or "MODERATE" or "WEAK",
  "trading_mode": "swing" or "intraday" or "positional",
  "confidence": 0.0-1.0,
  "entry_price": <number>,
  "stop_loss": <number>,
  "target_1": <number>,
  "target_2": <number>,
  "target_3": <number>,
  "reasoning": "<2-3 sentence explanation of why this signal>",
  "technical_summary": "<key technical factors driving this signal>",
  "fundamental_summary": "<sector and fundamental context>",
  "risk_reward": <number>
}}

Rules:
- Entry must be within 1% of current price
- For BUY: stop loss is 2-4% BELOW entry (swing) or 1-1.5% below (intraday);
  targets are ABOVE entry (T1: 3-5% swing / 1.5-2% intraday, T2: 5-8%, T3: 8-15%)
- For SELL: stop loss is 2-4% ABOVE entry (swing) or 1-1.5% above (intraday);
  targets are BELOW entry (T1: 3-5% swing / 1.5-2% intraday, T2: 5-8%, T3: 8-15%)
- For HOLD: set entry=current_price, stop_loss=entry, targets=entry
- Risk/reward must be at least 2:1
- Confidence below 0.65 → always HOLD
- News from last 24h carries 2x weight vs older news
- Strong bearish news + bearish technicals → SELL
- Strong bullish news + bullish technicals → BUY
- Conflicting signals or weak conviction → HOLD
- Be specific about NSE/BSE context and Indian market factors"""


class StockReasoningEngine:
    """DeepSeek V3 via OpenAI-compatible REST API. Switched from Gemini 2.5
    Flash on 2026-04-09 to reduce API costs ($90/5 days on Gemini).
    DeepSeek is ~10-20x cheaper per token."""

    DEEPSEEK_CHAT_URL = "https://api.deepseek.com/chat/completions"

    def __init__(self):
        self.api_key = settings.DEEPSEEK_API_KEY

    def analyse(self, stock: dict, quote: dict, tech: dict, news_context: str = "", news_stats: dict = None) -> Optional[StockSignal]:
        current = quote["current_price"]
        news_stats = news_stats or {"bias": "neutral", "total": 0}
        prompt = STOCK_REASONING_PROMPT.format(
            symbol=stock["symbol"],
            name=stock["name"],
            sector=stock["sector"],
            current_price=f"{current:,.2f}",
            day_low=f"{quote['day_low']:,.2f}",
            day_high=f"{quote['day_high']:,.2f}",
            w52l=f"{quote['52w_low']:,.2f}",
            w52h=f"{quote['52w_high']:,.2f}",
            volume=f"{quote['volume']:,}",
            volume_signal=tech["volume"],
            pe_ratio=quote.get("pe_ratio") or "N/A",
            rsi=tech["rsi"],
            rsi_signal=tech["rsi_signal"],
            macd_val=tech["macd"]["macd"],
            macd_signal=tech["macd"]["signal"],
            macd_crossover=tech["macd"]["crossover"],
            trend=tech["trend"],
            support=f"{tech['support']:,.2f}",
            resistance=f"{tech['resistance']:,.2f}",
            pivot=f"{tech['pivot']:,.2f}",
            from_52h=tech["from_52w_high"],
            from_52l=tech["from_52w_low"],
            news_context=news_context or "No recent news. Base analysis on technicals only.",
            news_bias=news_stats.get("bias", "neutral"),
            news_total=news_stats.get("total", 0),
        )
        try:
            import httpx
            resp = httpx.post(
                self.DEEPSEEK_CHAT_URL,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": settings.DEEPSEEK_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.3,
                    "max_tokens": 1500,
                    "response_format": {"type": "json_object"},
                },
                timeout=60,
            )
            resp.raise_for_status()
            result = resp.json()
            raw = result["choices"][0]["message"]["content"].strip()
            data = json.loads(raw)
            confidence = float(data.get("confidence", 0.5))
            if confidence < settings.MIN_CONFIDENCE:
                data["signal"] = "HOLD"
            # Overbought guardrail: a BUY into RSI>65 needs strong conviction.
            # Counteracts the LLM's observed BUY bias when momentum is stretched.
            if tech["rsi"] > 65 and data.get("signal") == "BUY" and confidence <= 0.75:
                logger.debug(f"{stock['symbol']}: BUY downgraded to HOLD (RSI {tech['rsi']} > 65, conf {confidence:.2f})")
                data["signal"] = "HOLD"
            entry = float(data.get("entry_price", current))
            # Direction-aware defaults and risk/reward math — SELL flips signs.
            sig_dir = data.get("signal", "HOLD")
            if sig_dir == "SELL":
                sl = float(data.get("stop_loss", entry * 1.03))
                t1 = float(data.get("target_1", entry * 0.96))
                t2_default = entry * 0.93
                t3_default = entry * 0.88
                risk = sl - entry
                reward = entry - t1
            else:
                # BUY and HOLD use long-side math; HOLD will have zero risk/reward anyway
                sl = float(data.get("stop_loss", entry * 0.97))
                t1 = float(data.get("target_1", entry * 1.04))
                t2_default = entry * 1.07
                t3_default = entry * 1.12
                risk = entry - sl
                reward = t1 - entry
            rr = round(reward / risk, 2) if risk > 0 else 0
            pos_pct = min(settings.MAX_POSITION_PCT, confidence * 0.05)
            pos_inr = settings.STARTING_CAPITAL * pos_pct
            signal = StockSignal(
                symbol=stock["symbol"],
                company_name=stock["name"],
                exchange="NSE",
                signal=data.get("signal", "HOLD"),
                strength=data.get("strength", "WEAK"),
                trading_mode=data.get("trading_mode", "swing"),
                current_price=current,
                entry_price=entry,
                stop_loss=sl,
                target_1=t1,
                target_2=float(data.get("target_2", t2_default)),
                target_3=float(data.get("target_3", t3_default)),
                risk_reward=rr,
                position_size_pct=pos_pct,
                position_size_inr=round(pos_inr, 0),
                confidence=confidence,
                reasoning=data.get("reasoning", ""),
                technical_summary=data.get("technical_summary", ""),
                fundamental_summary=data.get("fundamental_summary", ""),
                rsi=tech["rsi"],
                macd_signal=tech["macd"]["crossover"],
                trend=tech["trend"],
                sector=stock["sector"],
                pe_ratio=quote.get("pe_ratio"),
            )
            logger.info(
                f"{stock['symbol']:12} | {signal.signal:4} {signal.strength:8} | "
                f"₹{current:,.0f} | Entry: ₹{entry:,.0f} | SL: ₹{sl:,.0f} | "
                f"T1: ₹{t1:,.0f} | R:R {rr} | Conf: {confidence:.0%}"
            )
            return signal
        except Exception as e:
            logger.error(f"Reasoning failed for {stock['symbol']}: {e}")
            return None


# ── Storage ───────────────────────────────────────────────────────────────────
class Storage:
    def __init__(self):
        Path(settings.DB_PATH).parent.mkdir(parents=True, exist_ok=True)

    def _connect(self):
        from turso_client import connect
        return connect(settings.DB_PATH)

    async def init(self):
        async with self._connect() as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id TEXT PRIMARY KEY, symbol TEXT, company_name TEXT,
                    signal TEXT, strength TEXT, trading_mode TEXT,
                    current_price REAL, entry_price REAL, stop_loss REAL,
                    target_1 REAL, target_2 REAL, target_3 REAL,
                    risk_reward REAL, position_size_inr REAL, confidence REAL,
                    reasoning TEXT, technical_summary TEXT, fundamental_summary TEXT,
                    rsi REAL, macd_signal TEXT, trend TEXT, sector TEXT,
                    pe_ratio REAL, timestamp TEXT
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS paper_trades (
                    id TEXT PRIMARY KEY, symbol TEXT, signal TEXT,
                    entry_price REAL, stop_loss REAL, target_1 REAL,
                    size_inr REAL, status TEXT DEFAULT 'open',
                    exit_price REAL, pnl_inr REAL, pnl_pct REAL,
                    entered_at TEXT, exited_at TEXT
                )
            """)
            await db.commit()

    async def save_signal(self, s: StockSignal):
        async with self._connect() as db:
            await db.execute("""INSERT OR REPLACE INTO signals VALUES
                (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                str(uuid.uuid4()), s.symbol, s.company_name, s.signal,
                s.strength, s.trading_mode, s.current_price, s.entry_price,
                s.stop_loss, s.target_1, s.target_2, s.target_3,
                s.risk_reward, s.position_size_inr, s.confidence,
                s.reasoning, s.technical_summary, s.fundamental_summary,
                s.rsi, s.macd_signal, s.trend, s.sector,
                s.pe_ratio, s.timestamp.isoformat(),
            ))
            await db.commit()

    async def get_recent_signals(self, limit: int = 20) -> list:
        async with self._connect() as db:
            db.row_factory = True
            async with db.execute(
                "SELECT * FROM signals ORDER BY timestamp DESC LIMIT ?", (limit,)
            ) as c:
                return [dict(r) for r in await c.fetchall()]

    async def open_paper_trade(self, signal: "StockSignal"):
        async with self._connect() as db:
            # Skip if already have open position in this stock
            async with db.execute(
                "SELECT COUNT(*) as cnt FROM paper_trades WHERE symbol=? AND status='open'",
                (signal.symbol,)
            ) as c:
                if (await c.fetchone())[0] > 0:
                    return None
            tid = str(uuid.uuid4())
            target = signal.target_1 if signal.signal == "BUY" else signal.target_1
            await db.execute("""INSERT INTO paper_trades VALUES
                (?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                tid, signal.symbol, signal.signal,
                signal.entry_price, signal.stop_loss, target,
                signal.position_size_inr, "open",
                None, None, None,
                datetime.now().isoformat(), None,
            ))
            await db.commit()
            logger.info(
                f"PAPER TRADE OPENED: {signal.signal} {signal.symbol} | "
                f"Rs {signal.position_size_inr:,.0f} @ Rs {signal.entry_price:,.0f} | "
                f"SL: Rs {signal.stop_loss:,.0f} | T1: Rs {target:,.0f}"
            )
            return tid

    async def check_open_trades(self, fetcher: "StockDataFetcher"):
        """Check open trades against current prices, close if SL or T1 hit."""
        async with self._connect() as db:
            db.row_factory = True
            async with db.execute("SELECT * FROM paper_trades WHERE status='open'") as c:
                trades = [dict(r) for r in await c.fetchall()]

        closed = 0
        for trade in trades:
            quote = await fetcher.get_quote(trade["symbol"])
            if not quote:
                continue
            price = quote["current_price"]
            entry = trade["entry_price"]
            sl = trade["stop_loss"]
            t1 = trade["target_1"]
            is_buy = trade["signal"] == "BUY"

            hit_sl = (price <= sl) if is_buy else (price >= sl)
            hit_t1 = (price >= t1) if is_buy else (price <= t1)

            if hit_sl or hit_t1:
                exit_price = sl if hit_sl else t1
                if is_buy:
                    pnl_pct = (exit_price - entry) / entry
                else:
                    pnl_pct = (entry - exit_price) / entry
                pnl_inr = trade["size_inr"] * pnl_pct
                result = "T1 HIT" if hit_t1 else "SL HIT"

                async with self._connect() as db:
                    await db.execute("""UPDATE paper_trades SET
                        status='closed', exit_price=?, pnl_inr=?, pnl_pct=?, exited_at=?
                        WHERE id=?""", (
                        exit_price, round(pnl_inr, 2), round(pnl_pct, 4),
                        datetime.now().isoformat(), trade["id"],
                    ))
                    await db.commit()

                logger.info(
                    f"PAPER TRADE CLOSED: {result} | {trade['signal']} {trade['symbol']} | "
                    f"Entry: Rs {entry:,.0f} -> Exit: Rs {exit_price:,.0f} | "
                    f"P&L: Rs {pnl_inr:+,.0f} ({pnl_pct:+.1%})"
                )
                closed += 1
        return closed

    async def get_open_trades(self) -> list:
        async with self._connect() as db:
            db.row_factory = True
            async with db.execute("SELECT * FROM paper_trades WHERE status='open' ORDER BY entered_at DESC") as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_closed_trades(self) -> list:
        async with self._connect() as db:
            db.row_factory = True
            async with db.execute("SELECT * FROM paper_trades WHERE status='closed' ORDER BY exited_at DESC") as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_portfolio_summary(self) -> dict:
        async with self._connect() as db:
            db.row_factory = True
            async with db.execute("SELECT COUNT(*) as total FROM paper_trades") as c:
                total = (await c.fetchone())["total"]
            async with db.execute("SELECT COUNT(*) as open FROM paper_trades WHERE status='open'") as c:
                open_count = (await c.fetchone())["open"]
            async with db.execute("SELECT COUNT(*) as closed FROM paper_trades WHERE status='closed'") as c:
                closed_count = (await c.fetchone())["closed"]
            async with db.execute("SELECT COUNT(*) as wins FROM paper_trades WHERE status='closed' AND pnl_inr > 0") as c:
                wins = (await c.fetchone())["wins"]
            async with db.execute("SELECT COALESCE(SUM(pnl_inr), 0) as pnl FROM paper_trades WHERE status='closed'") as c:
                total_pnl = (await c.fetchone())["pnl"]
            async with db.execute("SELECT COALESCE(SUM(size_inr), 0) as deployed FROM paper_trades WHERE status='open'") as c:
                deployed = (await c.fetchone())["deployed"]
        win_rate = (wins / closed_count) if closed_count > 0 else 0
        return {
            "total_trades": total, "open_trades": open_count, "closed_trades": closed_count,
            "wins": wins, "win_rate": round(win_rate, 4),
            "total_pnl": round(total_pnl, 2), "deployed": round(deployed, 2),
            "capital": settings.STARTING_CAPITAL,
        }


# ── Main Engine ───────────────────────────────────────────────────────────────
class StockBotEngine:
    def __init__(self):
        self.fetcher = StockDataFetcher()
        self.tech = TechnicalAnalyser()
        self.reasoner = StockReasoningEngine()
        self.storage = Storage()
        self.news = None
        self._running = False

    async def startup(self):
        from news_fetcher import NewsIntelligence
        self.news = NewsIntelligence()
        await self.storage.init()
        logger.info(f"NSE Stock Bot started | Watching {len(NSE_WATCHLIST)} stocks")
        logger.info(f"Capital: ₹{settings.STARTING_CAPITAL:,.0f} | Max per trade: {settings.MAX_POSITION_PCT:.0%}")
        logger.info("News: MoneyControl + ET + Business Standard + Google News")

    async def scan_stock(self, stock: dict) -> Optional[StockSignal]:
        quote = await self.fetcher.get_quote(stock["symbol"])
        if not quote:
            return None
        tech = self.tech.full_analysis(quote)
        news_context = ""
        news_stats = {"bias": "neutral", "total": 0}
        if self.news:
            try:
                news_items = await self.news.get_stock_news(stock["symbol"], stock["name"])
                news_context = self.news.format_for_prompt(news_items)
                news_stats = self.news.get_news_summary_stats(news_items)
                if news_items:
                    logger.debug(f"{stock['symbol']} news: {len(news_items)} articles, bias={news_stats['bias']}")
            except Exception as e:
                logger.debug(f"News fetch failed for {stock['symbol']}: {e}")
        signal = self.reasoner.analyse(stock, quote, tech, news_context, news_stats)
        if signal:
            await self.storage.save_signal(signal)
        return signal

    async def run_scan(self):
        if not self.is_market_hours():
            logger.info("Market closed - skipping scan")
            return []
        # Check existing positions for SL/T1 hits
        try:
            closed = await self.storage.check_open_trades(self.fetcher)
            if closed:
                logger.info(f"Closed {closed} paper trades (SL/T1 hit)")
        except Exception as e:
            logger.error(f"Error checking open trades: {e}")

        logger.info(f"=== NSE Scan started - {len(NSE_WATCHLIST)} stocks ===")
        signals = []
        for stock in NSE_WATCHLIST:
            try:
                sig = await self.scan_stock(stock)
                if sig and sig.signal in ("BUY", "SELL"):
                    signals.append(sig)
                    try:
                        await self.storage.open_paper_trade(sig)
                    except Exception as e:
                        logger.error(f"Failed to open paper trade for {sig.symbol}: {e}")
            except Exception as e:
                logger.error(f"Error scanning {stock['symbol']}: {e}")
            await asyncio.sleep(1.0)  # Rate limit: ~100 stocks per scan needs spacing
        buys = [s for s in signals if s.signal == "BUY"]
        sells = [s for s in signals if s.signal == "SELL"]
        # Portfolio summary
        portfolio = await self.storage.get_portfolio_summary()
        logger.info(
            f"Scan complete | BUY: {len(buys)} | SELL: {len(sells)} | "
            f"Open: {portfolio['open_trades']} | Closed: {portfolio['closed_trades']} | "
            f"Win rate: {portfolio['win_rate']:.0%} | P&L: Rs {portfolio['total_pnl']:+,.0f}"
        )
        for s in sorted(signals, key=lambda x: x.confidence, reverse=True):
            logger.info(
                f"  {s.signal} {s.symbol} | Rs {s.entry_price:,.0f} -> "
                f"SL Rs {s.stop_loss:,.0f} | T1 Rs {s.target_1:,.0f} | "
                f"Conf: {s.confidence:.0%} | {s.trading_mode}"
            )
        return signals

    @staticmethod
    def is_market_hours() -> bool:
        """Check if NSE is open (9:15 AM - 3:00 PM IST, Mon-Fri)."""
        ist = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
        h, m = ist.hour, ist.minute
        day = ist.weekday()  # 0=Mon, 4=Fri
        return day < 5 and (h > 9 or (h == 9 and m >= 15)) and h < 15

    async def run(self):
        self._running = True
        await self.startup()
        while self._running:
            if self.is_market_hours():
                await self.run_scan()
                logger.info(f"Next scan in {settings.SCAN_INTERVAL_MINUTES} minutes...")
                await asyncio.sleep(settings.SCAN_INTERVAL_MINUTES * 60)
            else:
                ist = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
                logger.info(f"Market closed (IST: {ist.strftime('%H:%M %A')}). Sleeping 15 min...")
                await asyncio.sleep(900)

    async def run_once(self):
        await self.startup()
        return await self.run_scan()

    async def shutdown(self):
        self._running = False
        await self.fetcher.close()


if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    engine = StockBotEngine()
    if cmd == "once":
        asyncio.run(engine.run_once())
    elif cmd == "run":
        asyncio.run(engine.run())
