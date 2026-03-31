"""
web_dashboard.py - FastAPI dashboard for NSE Stock Bot
Run with: python web_dashboard.py
"""

import asyncio
import aiosqlite
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
from stock_bot import Storage, settings

storage = Storage()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await storage.init()
    yield


app = FastAPI(title="NSE Stock Bot Dashboard", lifespan=lifespan)


@app.get("/api/signals")
async def api_signals():
    return await storage.get_recent_signals(50)


@app.get("/api/signals/actionable")
async def api_actionable():
    async with aiosqlite.connect(settings.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM signals WHERE signal IN ('BUY','SELL') ORDER BY timestamp DESC LIMIT 30"
        ) as c:
            return [dict(r) for r in await c.fetchall()]


@app.get("/api/stats")
async def api_stats():
    async with aiosqlite.connect(settings.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
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
    async with aiosqlite.connect(settings.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT sector, signal, COUNT(*) as cnt, AVG(confidence) as avg_conf
            FROM signals WHERE signal IN ('BUY','SELL')
            GROUP BY sector, signal ORDER BY cnt DESC
        """) as c:
            return [dict(r) for r in await c.fetchall()]


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
    <button class="tab active" onclick="showTab('signals')">All Signals</button>
    <button class="tab" onclick="showTab('buysell')">BUY / SELL Only</button>
    <button class="tab" onclick="showTab('sectors')">Sector View</button>
  </div>

  <div id="tab-signals" class="tbl-wrap">
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
  document.getElementById('tab-' + name).style.display = name === 'sectors' ? 'block' : 'block';
  event.target.classList.add('active');
}

async function refresh() {
  await Promise.all([loadStats(), loadSignals(), loadActionable(), loadSectors()]);
}
refresh();
setInterval(refresh, 30000);

let cd = 30;
setInterval(() => { cd--; if (cd <= 0) cd = 30; document.getElementById('timer').textContent = 'Auto-refresh: ' + cd + 's'; }, 1000);
</script>
</body>
</html>"""


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8060)
