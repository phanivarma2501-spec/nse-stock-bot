"""
NSE/BSE Stock Trading Bot — Phase 1 (Paper Trading)
Claude-powered AI reasoning for Indian stock markets.

Covers: Swing, Intraday, Positional trading
Signals: Buy/Sell + Entry + Stop Loss + Target
Analysis: Technical (RSI, MACD, EMA) + Fundamental (PE, earnings)
"""

import anthropic
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
    ANTHROPIC_API_KEY: str = ""
    PHASE: int = 1
    PAPER_TRADING: bool = True
    LIVE_TRADING_ENABLED: bool = False
    STARTING_CAPITAL: float = 100000.0  # ₹1 lakh default
    DB_PATH: str = "data/stock_bot.db"
    SCAN_INTERVAL_MINUTES: int = 60
    MIN_CONFIDENCE: float = 0.65
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
    # Large Cap
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
    # Mid Cap
    {"symbol": "PERSISTENT", "name": "Persistent Systems", "sector": "IT", "market_cap": "mid"},
    {"symbol": "DIXON", "name": "Dixon Technologies", "sector": "Electronics", "market_cap": "mid"},
    {"symbol": "IRCTC", "name": "IRCTC", "sector": "Travel", "market_cap": "mid"},
    {"symbol": "POLYCAB", "name": "Polycab India", "sector": "Cables", "market_cap": "mid"},
    {"symbol": "ZOMATO", "name": "Zomato", "sector": "Food-Tech", "market_cap": "mid"},
]


# ── Yahoo Finance Data Fetcher ────────────────────────────────────────────────
class StockDataFetcher:
    """Fetches stock data via yfinance (handles Yahoo auth automatically)."""

    def __init__(self):
        import yfinance as yf
        self.yf = yf

    async def get_quote(self, symbol: str) -> Optional[dict]:
        """Get current price and 3-month history."""
        yf_symbol = f"{symbol}.NS"
        try:
            ticker = self.yf.Ticker(yf_symbol)
            hist = ticker.history(period="3mo")
            if hist.empty:
                return None

            closes = hist["Close"].dropna().tolist()
            highs = hist["High"].dropna().tolist()
            lows = hist["Low"].dropna().tolist()
            volumes = hist["Volume"].dropna().tolist()

            if not closes:
                return None

            info = ticker.info or {}
            current_price = info.get("currentPrice") or info.get("regularMarketPrice") or closes[-1]

            return {
                "symbol": symbol,
                "current_price": current_price,
                "prev_close": info.get("previousClose", closes[-2] if len(closes) > 1 else current_price),
                "day_high": info.get("dayHigh", highs[-1] if highs else current_price),
                "day_low": info.get("dayLow", lows[-1] if lows else current_price),
                "volume": info.get("volume", volumes[-1] if volumes else 0),
                "52w_high": info.get("fiftyTwoWeekHigh", max(highs) if highs else current_price),
                "52w_low": info.get("fiftyTwoWeekLow", min(lows) if lows else current_price),
                "market_cap": info.get("marketCap", 0),
                "pe_ratio": info.get("trailingPE"),
                "closes": closes[-60:],
                "highs": highs[-60:],
                "lows": lows[-60:],
                "volumes": volumes[-60:],
            }
        except Exception as e:
            logger.warning(f"Failed to fetch {symbol}: {e}")
            return None

    async def close(self):
        pass


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
Nifty50 trend: Generally bullish in 2026
Sector rotation: {sector} sector current conditions
FII/DII: Net buyers in recent sessions

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
- Stop loss: 2-4% below entry for swing, 1-1.5% for intraday
- Target 1: 3-5% above entry (swing), 1.5-2% intraday
- Target 2: 5-8% above entry
- Target 3: 8-15% above entry (positional)
- Risk/reward must be at least 2:1
- Confidence below 0.65 → always HOLD
- News from last 24h carries 2x weight vs older news
- Strong bearish news overrides bullish technicals → HOLD or SELL
- Strong bullish news with bullish technicals → increase confidence
- Be specific about NSE/BSE context and Indian market factors"""


class StockReasoningEngine:
    def __init__(self):
        self.client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

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
            response = self.client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=1000,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = response.content[0].text.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            raw = raw.strip().rstrip("```").strip()
            data = json.loads(raw)
            confidence = float(data.get("confidence", 0.5))
            if confidence < settings.MIN_CONFIDENCE:
                data["signal"] = "HOLD"
            entry = float(data.get("entry_price", current))
            sl = float(data.get("stop_loss", entry * 0.97))
            t1 = float(data.get("target_1", entry * 1.04))
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
                target_2=float(data.get("target_2", entry * 1.07)),
                target_3=float(data.get("target_3", entry * 1.12)),
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

    async def init(self):
        async with aiosqlite.connect(settings.DB_PATH) as db:
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
        async with aiosqlite.connect(settings.DB_PATH) as db:
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
        async with aiosqlite.connect(settings.DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM signals ORDER BY timestamp DESC LIMIT ?", (limit,)
            ) as c:
                return [dict(r) for r in await c.fetchall()]


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
        logger.info(f"=== NSE Scan started - {len(NSE_WATCHLIST)} stocks ===")
        signals = []
        for stock in NSE_WATCHLIST:
            sig = await self.scan_stock(stock)
            if sig and sig.signal in ("BUY", "SELL"):
                signals.append(sig)
            await asyncio.sleep(0.5)  # Rate limiting
        buys = [s for s in signals if s.signal == "BUY"]
        sells = [s for s in signals if s.signal == "SELL"]
        logger.info(f"Scan complete | BUY: {len(buys)} | SELL: {len(sells)}")
        for s in sorted(signals, key=lambda x: x.confidence, reverse=True):
            logger.info(
                f"  {s.signal} {s.symbol} | ₹{s.entry_price:,.0f} → "
                f"SL ₹{s.stop_loss:,.0f} | T1 ₹{s.target_1:,.0f} | "
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
