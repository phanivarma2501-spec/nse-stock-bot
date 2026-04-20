"""
web_dashboard.py - FastAPI dashboard for NSE Stock Bot
Run with: python web_dashboard.py
"""

import asyncio
import aiosqlite
from fastapi import FastAPI
from turso_client import connect as turso_connect
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
from stock_bot import Storage, settings
from paper_trades_fo import FOStorage

storage = Storage()
fo_storage = FOStorage(settings.DB_PATH)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await storage.init()
        await fo_storage.init()
    except Exception as e:
        print(f"[WARNING] Storage init failed: {e} — dashboard will start but data may be empty", flush=True)
    yield


app = FastAPI(title="NSE Stock Bot Dashboard", lifespan=lifespan)


@app.get("/api/live-prices")
async def api_live_prices():
    """Fetch prices for all watchlist stocks. Uses NSE live when available,
    falls back to Yahoo (regular_market_price from chart metadata) when NSE
    is 403/blocked. The previous implementation called _fetch_nse_live
    directly with no fallback, which returned [] on every call from Railway
    because the IP is geo-blocked."""
    from stock_bot import StockDataFetcher, NSE_WATCHLIST
    fetcher = StockDataFetcher()

    async def quote_for(stock):
        try:
            q = await fetcher.get_quote(stock["symbol"])
            if not q:
                return None
            current = q["current_price"]
            prev = q.get("prev_close") or current
            change = current - prev
            change_pct = (change / prev * 100) if prev else 0
            return {
                "symbol": stock["symbol"],
                "name": stock["name"],
                "sector": stock["sector"],
                "price": current,
                "change": round(change, 2),
                "change_pct": round(change_pct, 2),
                "day_high": q.get("day_high") or current,
                "day_low": q.get("day_low") or current,
                "source": q.get("source", "UNKNOWN"),
            }
        except Exception:
            return None

    results = await asyncio.gather(*(quote_for(s) for s in NSE_WATCHLIST))
    await fetcher.close()
    return [r for r in results if r is not None]


@app.get("/api/signals")
async def api_signals():
    return await storage.get_recent_signals(50)


@app.get("/api/signals/actionable")
async def api_actionable():
    async with turso_connect(settings.DB_PATH) as db:
        db.row_factory = True
        async with db.execute(
            "SELECT * FROM signals WHERE signal IN ('BUY','SELL') ORDER BY timestamp DESC LIMIT 30"
        ) as c:
            return [dict(r) for r in await c.fetchall()]


@app.get("/api/trades/open")
async def api_open_trades():
    return await storage.get_open_trades()

@app.get("/api/trades/live")
async def api_trades_live():
    """Open trades with current NSE live prices and unrealized P&L."""
    from stock_bot import StockDataFetcher
    trades = await storage.get_open_trades()
    if not trades:
        return []

    fetcher = StockDataFetcher()
    price_cache = {}

    for trade in trades:
        symbol = trade["symbol"]
        if symbol not in price_cache:
            quote = await fetcher.get_quote(symbol)
            price_cache[symbol] = quote["current_price"] if quote else None

        current_price = price_cache.get(symbol)
        entry = trade["entry_price"]
        size = trade["size_inr"]
        is_buy = trade["signal"] == "BUY"

        if current_price:
            if is_buy:
                pnl_pct = (current_price - entry) / entry
            else:
                pnl_pct = (entry - current_price) / entry
            pnl_inr = size * pnl_pct
            trade["current_price"] = round(current_price, 2)
            trade["unrealized_pnl_inr"] = round(pnl_inr, 2)
            trade["unrealized_pnl_pct"] = round(pnl_pct * 100, 2)
        else:
            trade["current_price"] = None
            trade["unrealized_pnl_inr"] = None
            trade["unrealized_pnl_pct"] = None

    await fetcher.close()
    return trades

@app.get("/api/trades/closed")
async def api_closed_trades():
    return await storage.get_closed_trades()

@app.get("/api/portfolio")
async def api_portfolio():
    return await storage.get_portfolio_summary()


# ── F&O endpoints ─────────────────────────────────────────────────────────────
async def _fo_chain_summary(symbol: str, is_index: bool, expiry: str | None = None):
    """Shared helper — build OptionsChainFetcher per call so the dashboard has
    no long-lived NSE client holding cookies."""
    from options_chain import OptionsChainFetcher, summarize_chain
    f = OptionsChainFetcher()
    try:
        raw = await f.fetch_chain(symbol, is_index)
        if not raw:
            return None
        return summarize_chain(raw, expiry)
    finally:
        await f.close()


@app.get("/api/fo-summary")
async def api_fo_summary():
    return await fo_storage.get_summary()


@app.get("/api/fo-trades/live")
async def api_fo_live():
    """Open F&O trades with current premium + unrealized P&L marked against NSE chain."""
    async def summarize_for(symbol, is_index, expiry):
        return await _fo_chain_summary(symbol, is_index, expiry)
    return await fo_storage.mark_open_pnl(summarize_for)


@app.get("/api/fo-trades/closed")
async def api_fo_closed():
    return await fo_storage.get_closed_trades(100)


@app.get("/api/fo-signals")
async def api_fo_signals():
    """Today's F&O recommendations: runs the strategy engine against current chains
    for the F&O universe (NIFTY, BANKNIFTY, 20 stocks) using the latest saved equity signal."""
    from options_chain import INDICES, FNO_STOCKS, compute_pcr, compute_max_pain, iv_rank_in_chain, iv_regime
    from options_strategy import recommend

    async def latest_signal(symbol):
        async with turso_connect(settings.DB_PATH) as db:
            db.row_factory = True
            async with db.execute(
                "SELECT signal, strength, confidence FROM signals WHERE symbol=? "
                "ORDER BY timestamp DESC LIMIT 1",
                (symbol,),
            ) as c:
                row = await c.fetchone()
        return dict(row) if row else {"signal": "HOLD", "strength": "WEAK", "confidence": 0.5}

    out = []
    for symbol, is_index in [(s, True) for s in INDICES] + [(s, False) for s in FNO_STOCKS]:
        summary = await _fo_chain_summary(symbol, is_index)
        if not summary:
            continue
        sig = await latest_signal(symbol)
        rec = recommend(symbol, sig, summary, is_index)
        row = {
            "symbol": symbol,
            "is_index": is_index,
            "spot": summary["spot"],
            "atm_strike": summary["atm_strike"],
            "atm_iv": round(summary["atm_iv"], 2),
            "iv_regime": iv_regime(summary["atm_iv"], is_index),
            "iv_rank_in_chain": iv_rank_in_chain(summary),
            "pcr": compute_pcr(summary),
            "max_pain": compute_max_pain(summary),
            "expiry": summary["expiry"],
            "equity_signal": sig.get("signal"),
            "equity_strength": sig.get("strength"),
            "recommendation": rec,  # may be None → "no trade"
        }
        out.append(row)
    return out

@app.get("/api/stats")
async def api_stats():
    async with turso_connect(settings.DB_PATH) as db:
        db.row_factory = True
        async with db.execute("SELECT COUNT(*) as total FROM signals") as c:
            total = (await c.fetchone())["total"]
        async with db.execute("SELECT COUNT(*) as buys FROM signals WHERE signal='BUY'") as c:
            buys = (await c.fetchone())["buys"]
        async with db.execute("SELECT COUNT(*) as sells FROM signals WHERE signal='SELL'") as c:
            sells = (await c.fetchone())["sells"]
        async with db.execute("SELECT COUNT(*) as holds FROM signals WHERE signal='HOLD'") as c:
            holds = (await c.fetchone())["holds"]
        async with db.execute("SELECT COUNT(DISTINCT symbol) as stocks FROM signals") as c:
            stocks = (await c.fetchone())["stocks"]
        async with db.execute("SELECT AVG(confidence) as avg_conf FROM signals WHERE signal IN ('BUY','SELL')") as c:
            avg_conf = (await c.fetchone())["avg_conf"] or 0
    return {
        "total_signals": total, "buys": buys, "sells": sells,
        "holds": holds, "stocks_tracked": stocks,
        "avg_confidence": round(avg_conf, 3),
        "capital": settings.STARTING_CAPITAL,
    }


@app.get("/api/sectors")
async def api_sectors():
    async with turso_connect(settings.DB_PATH) as db:
        db.row_factory = True
        async with db.execute("""
            SELECT sector, signal, COUNT(*) as cnt, AVG(confidence) as avg_conf
            FROM signals WHERE signal IN ('BUY','SELL')
            GROUP BY sector, signal ORDER BY cnt DESC
        """) as c:
            return [dict(r) for r in await c.fetchall()]


@app.get("/api/debug")
async def api_debug():
    """Debug endpoint — shows bot thread status and last errors."""
    import threading
    from stock_bot import StockBotEngine
    try:
        from server import _bot_last_error
    except ImportError:
        _bot_last_error = {"error": None, "time": None, "restart_count": 0}
    turso_ok = False
    turso_error = None
    try:
        async with turso_connect(settings.DB_PATH) as db:
            async with db.execute("SELECT COUNT(*) as cnt FROM signals") as c:
                await c.fetchone()
            turso_ok = True
    except Exception as e:
        turso_error = str(e)
    threads = [t.name for t in threading.enumerate()]
    bot_alive = any("bot" in t.lower() for t in threads)
    is_market = StockBotEngine.is_market_hours()
    return {
        "bot_thread_alive": bot_alive,
        "market_hours": is_market,
        "turso_connected": turso_ok,
        "turso_error": turso_error,
        "last_error": _bot_last_error["error"],
        "last_error_time": _bot_last_error["time"],
        "restart_count": _bot_last_error["restart_count"],
        "threads": threads,
        "thread_count": len(threads),
        "gemini_key_set": bool(settings.GEMINI_API_KEY),
    }


@app.get("/api/status")
async def api_status():
    """Health check with DB connectivity."""
    import threading
    db_ok = False
    perf = {}
    try:
        perf = await storage.get_portfolio_summary()
        db_ok = True
    except Exception as e:
        perf = {"error": str(e)}
    threads = [t.name for t in threading.enumerate()]
    return {
        "status": "ok" if db_ok else "db_error",
        "db_connected": db_ok,
        "total_trades": perf.get("total_trades", 0),
        "open_trades": perf.get("open_trades", 0),
        "threads": threads,
        "message": "Waiting for first scan..." if perf.get("total_trades", 0) == 0 else "Bot is running",
    }


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return DASHBOARD_HTML


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NSE Stock Bot - Dashboard</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Segoe UI', system-ui, sans-serif; background: #0a0e17; color: #e1e5ee; }

  .header {
    background: linear-gradient(135deg, #1a1f2e 0%, #0d1117 100%);
    padding: 18px 28px; border-bottom: 1px solid #21262d;
    display: flex; justify-content: space-between; align-items: center;
  }
  .header h1 { font-size: 21px; color: #f0883e; }
  .header .badge { padding: 4px 12px; border-radius: 12px; font-size: 12px; font-weight: 600; }
  .badge-phase { background: #238636; color: #fff; }
  .badge-nse { background: #1f6feb; color: #fff; }
  .refresh-info { color: #8b949e; font-size: 12px; }

  /* Live Ticker Banner */
  .ticker-wrap {
    background: #0d1117; border-bottom: 1px solid #21262d;
    overflow: hidden; height: 38px; position: relative;
  }
  .ticker-strip {
    display: flex; align-items: center; height: 100%;
    animation: scroll-left 300s linear infinite;
    white-space: nowrap; width: max-content;
  }
  .ticker-strip:hover { animation-play-state: paused; }
  @keyframes scroll-left {
    0% { transform: translateX(0); }
    100% { transform: translateX(-50%); }
  }
  .ticker-item {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 0 20px; font-size: 13px; font-weight: 500;
    border-right: 1px solid #21262d;
  }
  .ticker-item .sym { color: #58a6ff; font-weight: 700; }
  .ticker-item .price { color: #e1e5ee; }
  .ticker-item .chg-up { color: #3fb950; }
  .ticker-item .chg-down { color: #f85149; }
  .ticker-label {
    position: absolute; left: 0; top: 0; z-index: 2;
    background: linear-gradient(90deg, #f0883e 0%, #da6d25 100%);
    color: #fff; font-size: 11px; font-weight: 700; letter-spacing: 0.5px;
    padding: 0 14px; height: 100%; display: flex; align-items: center;
    text-transform: uppercase;
  }
  .ticker-loading {
    display: flex; align-items: center; justify-content: center;
    height: 100%; color: #8b949e; font-size: 12px;
  }

  .stats-row {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(155px, 1fr));
    gap: 14px; padding: 18px 28px;
  }
  .stat {
    background: #161b22; border: 1px solid #21262d; border-radius: 10px;
    padding: 16px; text-align: center;
  }
  .stat .label { color: #8b949e; font-size: 11px; text-transform: uppercase; letter-spacing: 0.8px; }
  .stat .val { font-size: 26px; font-weight: 700; margin-top: 4px; }
  .green { color: #3fb950; } .red { color: #f85149; } .blue { color: #58a6ff; }
  .yellow { color: #d29922; } .orange { color: #f0883e; }

  .content { padding: 8px 28px 20px; }
  .tabs { display: flex; gap: 4px; margin-bottom: 12px; }
  .tab {
    padding: 8px 16px; background: #21262d; border: none; color: #8b949e;
    cursor: pointer; border-radius: 6px; font-size: 13px; transition: all 0.15s;
  }
  .tab:hover { background: #30363d; }
  .tab.active { background: #f0883e; color: #fff; }

  .tbl-wrap {
    overflow-x: auto; max-height: 520px; overflow-y: auto;
    border: 1px solid #21262d; border-radius: 8px;
  }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th {
    background: #161b22; color: #8b949e; text-align: left; padding: 10px 11px;
    font-weight: 600; text-transform: uppercase; font-size: 11px; letter-spacing: 0.4px;
    position: sticky; top: 0; z-index: 1;
  }
  td { padding: 9px 11px; border-bottom: 1px solid #161b22; }
  tr:hover { background: #161b22; }

  .sig {
    padding: 3px 10px; border-radius: 6px; font-size: 11px; font-weight: 700;
    display: inline-block; min-width: 50px; text-align: center;
  }
  .sig-buy { background: #238636; color: #fff; }
  .sig-sell { background: #da3633; color: #fff; }
  .sig-hold { background: #30363d; color: #8b949e; }
  .str-strong { border: 1px solid #3fb950; }
  .str-weak { opacity: 0.7; }

  .mode-badge {
    padding: 2px 8px; border-radius: 4px; font-size: 10px; font-weight: 600;
    text-transform: uppercase;
  }
  .mode-swing { background: #1f3d5c; color: #58a6ff; }
  .mode-intraday { background: #3d1f3d; color: #d2a8ff; }
  .mode-positional { background: #1f3d2e; color: #7ee787; }

  .rsi-over { color: #f85149; } .rsi-under { color: #3fb950; } .rsi-neutral { color: #8b949e; }
  .trend-up { color: #3fb950; } .trend-down { color: #f85149; } .trend-side { color: #d29922; }

  .sector-grid {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 12px;
  }
  .sector-card {
    background: #161b22; border: 1px solid #21262d; border-radius: 8px; padding: 14px;
  }
  .sector-card h3 { font-size: 14px; color: #c9d1d9; margin-bottom: 6px; }
  .sector-bar { height: 6px; border-radius: 3px; margin-top: 6px; }

  .footer { text-align: center; padding: 18px; color: #484f58; font-size: 12px; }
</style>
</head>
<body>

<div class="header">
  <div style="display:flex;align-items:center;gap:12px;">
    <h1>NSE Stock Bot</h1>
    <span class="badge badge-nse">NSE</span>
    <span class="badge badge-phase">PHASE 1 - PAPER</span>
  </div>
  <div style="display:flex;align-items:center;gap:16px;">
    <span class="refresh-info" id="timer">Auto-refresh: 30s</span>
  </div>
</div>

<div class="ticker-wrap" id="tickerWrap">
  <div class="ticker-label">LIVE</div>
  <div class="ticker-loading" id="tickerLoading">Loading live prices...</div>
  <div class="ticker-strip" id="tickerStrip" style="display:none;padding-left:50px;"></div>
</div>

<div class="stats-row" id="stats">
  <div class="stat"><div class="label">Total Signals</div><div class="val blue" id="sTotal">-</div></div>
  <div class="stat"><div class="label">BUY Signals</div><div class="val green" id="sBuys">-</div></div>
  <div class="stat"><div class="label">SELL Signals</div><div class="val red" id="sSells">-</div></div>
  <div class="stat"><div class="label">HOLD</div><div class="val" id="sHolds">-</div></div>
  <div class="stat"><div class="label">Stocks Tracked</div><div class="val orange" id="sStocks">-</div></div>
  <div class="stat"><div class="label">Avg Confidence</div><div class="val yellow" id="sConf">-</div></div>
  <div class="stat"><div class="label">Capital</div><div class="val blue" id="sCap">-</div></div>
</div>

<div class="content">
  <div class="tabs">
    <button class="tab active" onclick="showTab('trades')">Paper Trades</button>
    <button class="tab" onclick="showTab('signals')">All Signals</button>
    <button class="tab" onclick="showTab('buysell')">BUY / SELL Only</button>
    <button class="tab" onclick="showTab('sectors')">Sector View</button>
    <button class="tab" onclick="showTab('fno')">F&amp;O</button>
  </div>

  <div id="tab-trades" class="tbl-wrap">
    <div id="portfolioBar" style="padding:12px 16px;background:#161b22;border-bottom:1px solid #21262d;display:flex;gap:24px;font-size:13px;"></div>
    <div id="unrealizedBar" style="padding:10px 16px;background:#161b22;border-bottom:1px solid #21262d;font-size:13px;display:none;"></div>
    <h3 style="padding:12px 16px 4px;color:#c9d1d9;font-size:14px;">Open Positions (Live P&L)</h3>
    <table>
      <thead><tr><th>#</th><th>Side</th><th>Stock</th><th>Entry</th><th>Current</th><th>SL</th><th>T1</th><th>Size</th><th>P&L</th><th>P&L%</th><th>Opened</th></tr></thead>
      <tbody id="openBody"></tbody>
    </table>
    <h3 style="padding:12px 16px 4px;color:#c9d1d9;font-size:14px;">Closed Trades</h3>
    <table>
      <thead><tr><th>#</th><th>Side</th><th>Stock</th><th>Entry</th><th>Exit</th><th>Size</th><th>P&L</th><th>Result</th><th>Closed</th></tr></thead>
      <tbody id="closedBody"></tbody>
    </table>
  </div>

  <div id="tab-signals" class="tbl-wrap" style="display:none">
    <table>
      <thead><tr>
        <th>Stock</th><th>Signal</th><th>Strength</th><th>Mode</th>
        <th>CMP</th><th>Entry</th><th>SL</th><th>T1</th><th>T2</th><th>T3</th>
        <th>R:R</th><th>RSI</th><th>Trend</th><th>Conf</th><th>Time</th>
      </tr></thead>
      <tbody id="allBody"></tbody>
    </table>
  </div>

  <div id="tab-buysell" class="tbl-wrap" style="display:none">
    <table>
      <thead><tr>
        <th>Stock</th><th>Sector</th><th>Signal</th><th>Mode</th>
        <th>Entry</th><th>Stop Loss</th><th>Target 1</th><th>Target 2</th><th>Target 3</th>
        <th>R:R</th><th>Size (INR)</th><th>Conf</th><th>Reasoning</th>
      </tr></thead>
      <tbody id="actionBody"></tbody>
    </table>
  </div>

  <div id="tab-sectors" style="display:none">
    <div class="sector-grid" id="sectorGrid"></div>
  </div>

  <div id="tab-fno" class="tbl-wrap" style="display:none">
    <div id="foSummaryBar" style="padding:12px 16px;background:#161b22;border-bottom:1px solid #21262d;display:flex;gap:24px;font-size:13px;flex-wrap:wrap;"></div>
    <h3 style="padding:12px 16px 4px;color:#c9d1d9;font-size:14px;">Open F&amp;O Positions (Live P&amp;L)</h3>
    <table>
      <thead><tr>
        <th>#</th><th>Symbol</th><th>Strategy</th><th>Dir</th><th>Expiry</th>
        <th>Legs</th><th>Net Entry</th><th>Current</th><th>Target</th><th>SL</th>
        <th>Size</th><th>P&amp;L</th><th>P&amp;L%</th><th>Opened</th>
      </tr></thead>
      <tbody id="foOpenBody"></tbody>
    </table>

    <h3 style="padding:16px 16px 4px;color:#c9d1d9;font-size:14px;">Today's Recommendations &amp; Chain Analytics</h3>
    <table>
      <thead><tr>
        <th>Symbol</th><th>Spot</th><th>ATM</th><th>ATM IV</th><th>IV Regime</th>
        <th>IV Rank</th><th>PCR</th><th>Max Pain</th><th>Expiry</th>
        <th>Equity Sig</th><th>Strategy</th><th>Net Prem</th><th>Target</th><th>SL</th><th>R:R</th>
      </tr></thead>
      <tbody id="foSignalsBody"></tbody>
    </table>

    <h3 style="padding:16px 16px 4px;color:#c9d1d9;font-size:14px;">Closed F&amp;O Trades</h3>
    <table>
      <thead><tr>
        <th>#</th><th>Symbol</th><th>Strategy</th><th>Expiry</th>
        <th>Entry</th><th>Exit</th><th>Size</th><th>P&amp;L</th><th>P&amp;L%</th><th>Reason</th><th>Closed</th>
      </tr></thead>
      <tbody id="foClosedBody"></tbody>
    </table>
  </div>
</div>

<div class="footer">NSE Stock Bot v1 - Phase 1 Paper Trading - Auto-refreshes every 30 seconds</div>

<script>
const INR = v => '&#8377;' + Number(v).toLocaleString('en-IN', {maximumFractionDigits: 0});
const pct = v => (v * 100).toFixed(0) + '%';
function fmtDate(iso) {
  if (!iso) return '-';
  const d = new Date(iso);
  return d.toLocaleDateString('en-IN', {day:'2-digit',month:'short'}) + ' ' + d.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'});
}
function sigClass(s) { return s === 'BUY' ? 'sig-buy' : s === 'SELL' ? 'sig-sell' : 'sig-hold'; }
function strClass(s) { return s === 'STRONG' ? 'str-strong' : s === 'WEAK' ? 'str-weak' : ''; }
function modeClass(m) { return 'mode-' + (m || 'swing'); }
function rsiClass(v) { return v < 35 ? 'rsi-under' : v > 70 ? 'rsi-over' : 'rsi-neutral'; }
function trendClass(t) { return t === 'uptrend' ? 'trend-up' : t === 'downtrend' ? 'trend-down' : 'trend-side'; }

async function loadStats() {
  const d = await (await fetch('/api/stats')).json();
  document.getElementById('sTotal').textContent = d.total_signals;
  document.getElementById('sBuys').textContent = d.buys;
  document.getElementById('sSells').textContent = d.sells;
  document.getElementById('sHolds').textContent = d.holds;
  document.getElementById('sStocks').textContent = d.stocks_tracked;
  document.getElementById('sConf').textContent = pct(d.avg_confidence);
  document.getElementById('sCap').innerHTML = INR(d.capital);
}

async function loadSignals() {
  const items = await (await fetch('/api/signals')).json();
  document.getElementById('allBody').innerHTML = items.map(s => `<tr>
    <td><b>${s.symbol}</b><br><span style="color:#8b949e;font-size:11px">${s.company_name||''}</span></td>
    <td><span class="sig ${sigClass(s.signal)} ${strClass(s.strength)}">${s.signal}</span></td>
    <td>${s.strength||'-'}</td>
    <td><span class="mode-badge ${modeClass(s.trading_mode)}">${s.trading_mode||'-'}</span></td>
    <td>${INR(s.current_price)}</td>
    <td>${INR(s.entry_price)}</td>
    <td style="color:#f85149">${INR(s.stop_loss)}</td>
    <td style="color:#3fb950">${INR(s.target_1)}</td>
    <td style="color:#3fb950">${INR(s.target_2)}</td>
    <td style="color:#3fb950">${INR(s.target_3)}</td>
    <td>${s.risk_reward||'-'}</td>
    <td class="${rsiClass(s.rsi)}">${s.rsi ? s.rsi.toFixed(1) : '-'}</td>
    <td class="${trendClass(s.trend)}">${s.trend||'-'}</td>
    <td>${s.confidence ? pct(s.confidence) : '-'}</td>
    <td style="font-size:11px;color:#8b949e">${fmtDate(s.timestamp)}</td>
  </tr>`).join('');
}

async function loadActionable() {
  const items = await (await fetch('/api/signals/actionable')).json();
  if (!items.length) {
    document.getElementById('actionBody').innerHTML = '<tr><td colspan="13" style="text-align:center;color:#8b949e;padding:30px;">No BUY/SELL signals yet</td></tr>';
    return;
  }
  document.getElementById('actionBody').innerHTML = items.map(s => `<tr>
    <td><b>${s.symbol}</b></td>
    <td>${s.sector||'-'}</td>
    <td><span class="sig ${sigClass(s.signal)}">${s.signal}</span></td>
    <td><span class="mode-badge ${modeClass(s.trading_mode)}">${s.trading_mode||'-'}</span></td>
    <td>${INR(s.entry_price)}</td>
    <td style="color:#f85149">${INR(s.stop_loss)}</td>
    <td style="color:#3fb950">${INR(s.target_1)}</td>
    <td style="color:#3fb950">${INR(s.target_2)}</td>
    <td style="color:#3fb950">${INR(s.target_3)}</td>
    <td>${s.risk_reward||'-'}</td>
    <td>${INR(s.position_size_inr)}</td>
    <td>${s.confidence ? pct(s.confidence) : '-'}</td>
    <td style="max-width:250px;font-size:11px;color:#c9d1d9;white-space:normal;">${(s.reasoning||'').substring(0,120)}${(s.reasoning||'').length>120?'...':''}</td>
  </tr>`).join('');
}

async function loadSectors() {
  const items = await (await fetch('/api/sectors')).json();
  const sectors = {};
  items.forEach(r => {
    if (!sectors[r.sector]) sectors[r.sector] = {buys: 0, sells: 0, conf: 0};
    if (r.signal === 'BUY') { sectors[r.sector].buys = r.cnt; sectors[r.sector].conf = r.avg_conf; }
    if (r.signal === 'SELL') sectors[r.sector].sells = r.cnt;
  });
  const grid = document.getElementById('sectorGrid');
  grid.innerHTML = Object.entries(sectors).map(([name, d]) => {
    const total = d.buys + d.sells;
    const buyPct = total > 0 ? (d.buys / total * 100) : 50;
    return `<div class="sector-card">
      <h3>${name}</h3>
      <div style="display:flex;justify-content:space-between;font-size:12px;">
        <span class="green">BUY: ${d.buys}</span>
        <span class="red">SELL: ${d.sells}</span>
      </div>
      <div style="font-size:11px;color:#8b949e;margin-top:4px;">Avg Conf: ${d.conf ? pct(d.conf) : '-'}</div>
      <div class="sector-bar" style="background:linear-gradient(to right, #238636 ${buyPct}%, #da3633 ${buyPct}%);"></div>
    </div>`;
  }).join('');
  if (!Object.keys(sectors).length) grid.innerHTML = '<p style="color:#8b949e;padding:20px;">No sector data yet</p>';
}

function showTab(name) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('[id^="tab-"]').forEach(t => t.style.display = 'none');
  document.getElementById('tab-' + name).style.display = 'block';
  event.target.classList.add('active');
  if (name === 'fno') loadFo();
}

async function loadPortfolio() {
  const d = await(await fetch('/api/portfolio')).json();
  const bar = document.getElementById('portfolioBar');
  bar.innerHTML = `
    <span>Total: <b>${d.total_trades}</b></span>
    <span>Open: <b style="color:#d29922">${d.open_trades}</b></span>
    <span>Closed: <b>${d.closed_trades}</b></span>
    <span>Wins: <b style="color:#3fb950">${d.wins}</b></span>
    <span>Win Rate: <b style="color:${d.win_rate>=0.5?'#3fb950':'#f85149'}">${(d.win_rate*100).toFixed(0)}%</b></span>
    <span>P&L: <b style="color:${d.total_pnl>=0?'#3fb950':'#f85149'}">${INR(d.total_pnl)}</b></span>
    <span>Deployed: <b style="color:#58a6ff">${INR(d.deployed)}</b> / ${INR(d.capital)}</span>
  `;
}

async function loadOpenTrades() {
  const trades = await(await fetch('/api/trades/live')).json();
  const tbody = document.getElementById('openBody');
  const bar = document.getElementById('unrealizedBar');
  if (!trades.length) { tbody.innerHTML = '<tr><td colspan="11" style="text-align:center;color:#8b949e;padding:20px;">No open positions</td></tr>'; bar.style.display='none'; return; }
  let totalPnl = 0;
  trades.forEach(t => { if (t.unrealized_pnl_inr != null) totalPnl += t.unrealized_pnl_inr; });
  bar.style.display = 'flex';
  bar.innerHTML = `<span>Unrealized P&L: <b style="color:${totalPnl>=0?'#3fb950':'#f85149'}">${totalPnl>=0?'+':''}${INR(totalPnl)}</b></span><span style="margin-left:16px;color:#8b949e;font-size:11px;">Live NSE prices | Updates every 30s</span>`;
  tbody.innerHTML = trades.map((t,i) => {
    const pnl = t.unrealized_pnl_inr;
    const pnlPct = t.unrealized_pnl_pct;
    const pnlColor = pnl!=null ? (pnl>=0?'#3fb950':'#f85149') : '#8b949e';
    return `<tr>
    <td>${i+1}</td>
    <td><span class="sig ${sigClass(t.signal)}">${t.signal}</span></td>
    <td><b>${t.symbol}</b></td>
    <td>${INR(t.entry_price)}</td>
    <td style="font-weight:600">${t.current_price ? INR(t.current_price) : '-'}</td>
    <td style="color:#f85149">${INR(t.stop_loss)}</td>
    <td style="color:#3fb950">${INR(t.target_1)}</td>
    <td>${INR(t.size_inr)}</td>
    <td style="color:${pnlColor};font-weight:600">${pnl!=null?(pnl>=0?'+':'')+INR(pnl):'-'}</td>
    <td style="color:${pnlColor};font-size:11px">${pnlPct!=null?(pnlPct>=0?'+':'')+pnlPct.toFixed(2)+'%':'-'}</td>
    <td style="font-size:11px;color:#8b949e">${fmtDate(t.entered_at)}</td>
  </tr>`;}).join('');
}

async function loadClosedTrades() {
  const trades = await(await fetch('/api/trades/closed')).json();
  const tbody = document.getElementById('closedBody');
  if (!trades.length) { tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;color:#8b949e;padding:20px;">No closed trades yet - waiting for SL/T1 hits</td></tr>'; return; }
  tbody.innerHTML = trades.map((t,i) => `<tr>
    <td>${i+1}</td>
    <td><span class="sig ${sigClass(t.signal)}">${t.signal}</span></td>
    <td><b>${t.symbol}</b></td>
    <td>${INR(t.entry_price)}</td>
    <td>${INR(t.exit_price)}</td>
    <td>${INR(t.size_inr)}</td>
    <td style="color:${(t.pnl_inr||0)>=0?'#3fb950':'#f85149'}">${t.pnl_inr!=null?(t.pnl_inr>=0?'+':'')+INR(t.pnl_inr):'-'}</td>
    <td>${t.pnl_inr!=null?(t.pnl_inr>0?'WIN':'LOSS'):'-'}</td>
    <td style="font-size:11px;color:#8b949e">${fmtDate(t.exited_at)}</td>
  </tr>`).join('');
}

function fmtLegs(legs) {
  if (!legs) return '-';
  try {
    if (typeof legs === 'string') legs = JSON.parse(legs);
    return legs.map(l => `${l.action==='BUY'?'+':'-'}${l.type}${l.strike}@${Number(l.price).toFixed(1)}`).join(' / ');
  } catch(e) { return '-'; }
}

async function loadFoSummary() {
  const d = await (await fetch('/api/fo-summary')).json();
  document.getElementById('foSummaryBar').innerHTML = `
    <span>Total: <b>${d.total}</b></span>
    <span>Open: <b style="color:#d29922">${d.open}</b></span>
    <span>Closed: <b>${d.closed}</b></span>
    <span>Wins: <b style="color:#3fb950">${d.wins}</b></span>
    <span>Win Rate: <b style="color:${d.win_rate>=0.5?'#3fb950':'#f85149'}">${(d.win_rate*100).toFixed(0)}%</b></span>
    <span>P&L: <b style="color:${d.total_pnl>=0?'#3fb950':'#f85149'}">${INR(d.total_pnl)}</b></span>
    <span>Deployed: <b style="color:#58a6ff">${INR(d.deployed)}</b></span>
  `;
}

async function loadFoOpen() {
  const trades = await (await fetch('/api/fo-trades/live')).json();
  const tbody = document.getElementById('foOpenBody');
  if (!trades.length) { tbody.innerHTML = '<tr><td colspan="14" style="text-align:center;color:#8b949e;padding:20px;">No open F&amp;O positions</td></tr>'; return; }
  tbody.innerHTML = trades.map((t,i) => {
    const pnl = t.unrealized_pnl_inr, pnlPct = t.unrealized_pnl_pct;
    const pnlColor = pnl!=null ? (pnl>=0?'#3fb950':'#f85149') : '#8b949e';
    return `<tr>
      <td>${i+1}</td>
      <td><b>${t.symbol}</b></td>
      <td style="font-size:11px">${t.strategy}</td>
      <td>${t.direction||'-'}</td>
      <td style="font-size:11px">${t.expiry}</td>
      <td style="font-size:10px;color:#c9d1d9;max-width:200px;white-space:normal;">${fmtLegs(t.legs_json)}</td>
      <td>${Number(t.net_premium).toFixed(2)}</td>
      <td>${t.current_premium!=null?Number(t.current_premium).toFixed(2):'-'}</td>
      <td style="color:#3fb950">${Number(t.target).toFixed(2)}</td>
      <td style="color:#f85149">${Number(t.stop_loss).toFixed(2)}</td>
      <td>${INR(t.size_inr)}</td>
      <td style="color:${pnlColor};font-weight:600">${pnl!=null?(pnl>=0?'+':'')+INR(pnl):'-'}</td>
      <td style="color:${pnlColor};font-size:11px">${pnlPct!=null?(pnlPct>=0?'+':'')+pnlPct.toFixed(2)+'%':'-'}</td>
      <td style="font-size:11px;color:#8b949e">${fmtDate(t.entered_at)}</td>
    </tr>`;
  }).join('');
}

async function loadFoSignals() {
  const rows = await (await fetch('/api/fo-signals')).json();
  const tbody = document.getElementById('foSignalsBody');
  if (!rows.length) { tbody.innerHTML = '<tr><td colspan="15" style="text-align:center;color:#8b949e;padding:20px;">No F&amp;O data — chain fetch may be blocked (Railway IP).</td></tr>'; return; }
  tbody.innerHTML = rows.map(r => {
    const rec = r.recommendation;
    const regimeColor = r.iv_regime === 'high' ? '#f85149' : r.iv_regime === 'low' ? '#3fb950' : '#d29922';
    const stratCell = rec ? `<span style="color:#58a6ff;font-weight:600">${rec.strategy}</span>` : '<span style="color:#8b949e">no trade</span>';
    return `<tr>
      <td><b>${r.symbol}</b></td>
      <td>${Number(r.spot).toFixed(2)}</td>
      <td>${r.atm_strike}</td>
      <td>${Number(r.atm_iv).toFixed(1)}%</td>
      <td style="color:${regimeColor}">${r.iv_regime}</td>
      <td>${Number(r.iv_rank_in_chain).toFixed(0)}%</td>
      <td style="color:${r.pcr>1?'#f85149':'#3fb950'}">${r.pcr}</td>
      <td>${r.max_pain}</td>
      <td style="font-size:11px">${r.expiry}</td>
      <td><span class="sig ${sigClass(r.equity_signal)}">${r.equity_signal||'-'}</span></td>
      <td>${stratCell}</td>
      <td>${rec?Number(rec.net_premium).toFixed(2):'-'}</td>
      <td style="color:#3fb950">${rec?Number(rec.target).toFixed(2):'-'}</td>
      <td style="color:#f85149">${rec?Number(rec.stop_loss).toFixed(2):'-'}</td>
      <td>${rec?rec.risk_reward:'-'}</td>
    </tr>`;
  }).join('');
}

async function loadFoClosed() {
  const trades = await (await fetch('/api/fo-trades/closed')).json();
  const tbody = document.getElementById('foClosedBody');
  if (!trades.length) { tbody.innerHTML = '<tr><td colspan="11" style="text-align:center;color:#8b949e;padding:20px;">No closed F&amp;O trades yet</td></tr>'; return; }
  tbody.innerHTML = trades.map((t,i) => `<tr>
    <td>${i+1}</td>
    <td><b>${t.symbol}</b></td>
    <td style="font-size:11px">${t.strategy}</td>
    <td style="font-size:11px">${t.expiry}</td>
    <td>${Number(t.net_premium).toFixed(2)}</td>
    <td>${t.current_premium!=null?Number(t.current_premium).toFixed(2):'-'}</td>
    <td>${INR(t.size_inr)}</td>
    <td style="color:${(t.pnl_inr||0)>=0?'#3fb950':'#f85149'}">${t.pnl_inr!=null?(t.pnl_inr>=0?'+':'')+INR(t.pnl_inr):'-'}</td>
    <td>${t.pnl_pct!=null?(t.pnl_pct>=0?'+':'')+t.pnl_pct.toFixed(2)+'%':'-'}</td>
    <td style="font-size:11px;color:#8b949e">${t.close_reason||'-'}</td>
    <td style="font-size:11px;color:#8b949e">${fmtDate(t.exited_at)}</td>
  </tr>`).join('');
}

async function loadFo() {
  await Promise.all([loadFoSummary(), loadFoOpen(), loadFoSignals(), loadFoClosed()]);
}

async function loadTicker() {
  try {
    const prices = await (await fetch('/api/live-prices')).json();
    if (!prices.length) return;
    const strip = document.getElementById('tickerStrip');
    const loading = document.getElementById('tickerLoading');
    // Duplicate items for seamless loop
    const html = prices.map(p => {
      const up = p.change_pct >= 0;
      const arrow = up ? '&#9650;' : '&#9660;';
      const cls = up ? 'chg-up' : 'chg-down';
      return `<div class="ticker-item">
        <span class="sym">${p.symbol}</span>
        <span class="price">${INR(p.price)}</span>
        <span class="${cls}">${arrow} ${Math.abs(p.change_pct).toFixed(2)}%</span>
      </div>`;
    }).join('');
    strip.innerHTML = html + html;  // duplicate for infinite scroll
    loading.style.display = 'none';
    strip.style.display = 'flex';
  } catch(e) {
    console.log('Ticker error:', e);
  }
}

async function refresh() {
  await Promise.all([loadStats(), loadSignals(), loadActionable(), loadSectors(), loadPortfolio(), loadOpenTrades(), loadClosedTrades()]);
}
refresh();
loadTicker();
setInterval(refresh, 30000);
setInterval(loadTicker, 60000);  // refresh prices every 60s

let cd = 30;
setInterval(() => { cd--; if (cd <= 0) cd = 30; document.getElementById('timer').textContent = 'Auto-refresh: ' + cd + 's'; }, 1000);
</script>
</body>
</html>"""


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8060)
