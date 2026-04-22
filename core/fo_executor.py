"""F&O paper trade executor — Level 4 (agent-driven).

Tables:
  fo_trades       — one row per open/closed F&O position
  fo_scan_logs    — per-scan audit row

Lifecycle:
  open_trade(trade_spec)          # inserts 'open' row
  mark_and_close(fetch_summary_fn) # prices open trades, closes on target/SL/expiry
  mark_open_pnl(fetch_summary_fn)  # returns open trades annotated with live P&L
  get_open_trades / get_closed_trades / get_summary

Replaces paper_trades_fo.py with:
  - Agent-provenance fields (confidence, edge, reasoning)
  - fo_scan_logs audit table
  - Expanded schema per user spec
"""

import json
import uuid
from datetime import datetime
from loguru import logger
from turso_client import connect


class FOExecutor:
    def __init__(self, db_path: str):
        self._db_path = db_path

    def _connect(self):
        return connect(self._db_path)

    async def init(self):
        async with self._connect() as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS fo_trades (
                    id TEXT PRIMARY KEY,
                    symbol TEXT,
                    is_index INTEGER,
                    strategy TEXT,
                    direction TEXT,
                    strike REAL,
                    expiry_date TEXT,
                    expiry_type TEXT,
                    legs_json TEXT,
                    lots INTEGER,
                    lot_size INTEGER,
                    entry_price REAL,
                    current_price REAL,
                    target REAL,
                    stop_loss REAL,
                    confidence REAL,
                    edge REAL,
                    reasoning TEXT,
                    equity_signal TEXT,
                    atm_iv REAL,
                    iv_regime TEXT,
                    pcr REAL,
                    max_pain REAL,
                    notional_inr REAL,
                    status TEXT DEFAULT 'open',
                    pnl_inr REAL,
                    pnl_pct REAL,
                    close_reason TEXT,
                    opened_at TEXT,
                    closed_at TEXT
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS fo_scan_logs (
                    id TEXT PRIMARY KEY,
                    symbols_scanned INTEGER,
                    signals_found INTEGER,
                    trades_placed INTEGER,
                    errors INTEGER,
                    notes TEXT,
                    scanned_at TEXT
                )
            """)
            await db.commit()

    async def open_trade(self, trade: dict) -> str | None:
        """Insert one row. De-dupes on symbol+strategy+expiry_date while open."""
        async with self._connect() as db:
            async with db.execute(
                "SELECT COUNT(*) FROM fo_trades WHERE symbol=? AND strategy=? "
                "AND expiry_date=? AND status='open'",
                (trade["symbol"], trade["strategy"], trade["expiry_date"]),
            ) as c:
                row = await c.fetchone()
                if row and row[0] > 0:
                    return None

            tid = str(uuid.uuid4())
            await db.execute(
                """INSERT INTO fo_trades (
                    id, symbol, is_index, strategy, direction, strike,
                    expiry_date, expiry_type, legs_json, lots, lot_size,
                    entry_price, current_price, target, stop_loss,
                    confidence, edge, reasoning, equity_signal,
                    atm_iv, iv_regime, pcr, max_pain, notional_inr,
                    status, pnl_inr, pnl_pct, close_reason, opened_at, closed_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    tid, trade["symbol"], 1 if trade.get("is_index") else 0,
                    trade["strategy"], trade["direction"], trade["anchor_strike"],
                    trade["expiry_date"], trade.get("expiry_type", "WEEKLY"),
                    json.dumps(trade["legs"]),
                    trade["lots"], trade["lot_size"],
                    trade["net_premium"], None,
                    trade["target"], trade["stop_loss"],
                    trade["confidence"], trade.get("edge"), trade.get("reasoning", "")[:4000],
                    trade.get("equity_signal"),
                    trade.get("atm_iv"), trade.get("iv_regime"),
                    trade.get("pcr"), trade.get("max_pain"),
                    trade.get("notional_inr"),
                    "open", None, None, None,
                    datetime.now().isoformat(), None,
                ),
            )
            await db.commit()
            logger.info(
                f"FO L4 OPEN: {trade['strategy']} {trade['symbol']} "
                f"K={trade['anchor_strike']} exp={trade['expiry_date']} "
                f"lots={trade['lots']} net={trade['net_premium']} "
                f"conf={trade['confidence']}"
            )
            return tid

    async def log_scan(self, symbols: int, signals: int, placed: int, errors: int, notes: str = "") -> None:
        async with self._connect() as db:
            await db.execute(
                "INSERT INTO fo_scan_logs (id, symbols_scanned, signals_found, "
                "trades_placed, errors, notes, scanned_at) VALUES (?,?,?,?,?,?,?)",
                (str(uuid.uuid4()), symbols, signals, placed, errors, notes,
                 datetime.now().isoformat()),
            )
            await db.commit()

    async def mark_and_close(self, fetch_summary) -> int:
        """Price open trades, close on target/SL/expiry. Returns count closed."""
        trades = await self.get_open_trades()
        if not trades:
            return 0

        today = datetime.now().date()
        chain_cache: dict = {}
        closed = 0

        for trade in trades:
            symbol, expiry = trade["symbol"], trade["expiry_date"]
            is_index = bool(trade["is_index"])

            try:
                exp_date = datetime.strptime(expiry, "%d-%b-%Y").date()
            except ValueError:
                exp_date = None

            if exp_date and exp_date < today:
                await self._close(trade, None, "EXPIRED")
                closed += 1
                continue

            key = (symbol, expiry)
            summary = chain_cache.get(key)
            if summary is None:
                summary = await fetch_summary(symbol, is_index, expiry)
                chain_cache[key] = summary
            if summary is None:
                continue

            legs = json.loads(trade["legs_json"])
            current_net = _price_legs(legs, summary)
            if current_net is None:
                continue

            net_entry = trade["entry_price"]
            target = trade["target"]
            sl = trade["stop_loss"]

            if net_entry >= 0:
                hit_target = current_net >= target
                hit_sl = current_net <= sl
            else:
                hit_target = current_net >= target
                hit_sl = current_net <= sl

            if hit_target or hit_sl:
                await self._close(trade, current_net, "TARGET" if hit_target else "STOP_LOSS")
                closed += 1

        return closed

    async def _close(self, trade: dict, current_net: float | None, reason: str) -> None:
        net_entry = trade["entry_price"]
        lot_size = trade["lot_size"]
        lots = trade["lots"]

        if current_net is None:
            current_net = 0.0

        if net_entry >= 0:
            pnl_points = current_net - net_entry
        else:
            pnl_points = abs(net_entry) - abs(current_net)
        pnl_inr = pnl_points * lot_size * lots
        notional = trade.get("notional_inr") or (abs(net_entry) * lot_size * lots) or 1
        pnl_pct = (pnl_inr / notional * 100) if notional else 0

        async with self._connect() as db:
            await db.execute(
                "UPDATE fo_trades SET status='closed', current_price=?, pnl_inr=?, "
                "pnl_pct=?, closed_at=?, close_reason=? WHERE id=?",
                (round(current_net, 2), round(pnl_inr, 2), round(pnl_pct, 2),
                 datetime.now().isoformat(), reason, trade["id"]),
            )
            await db.commit()

        logger.info(
            f"FO L4 CLOSED [{reason}]: {trade['strategy']} {trade['symbol']} "
            f"entry={net_entry} exit={current_net:.2f} "
            f"P&L: Rs {pnl_inr:+,.0f} ({pnl_pct:+.1f}%)"
        )

    async def mark_open_pnl(self, fetch_summary) -> list:
        trades = await self.get_open_trades()
        chain_cache: dict = {}
        for trade in trades:
            key = (trade["symbol"], trade["expiry_date"])
            summary = chain_cache.get(key)
            if summary is None:
                summary = await fetch_summary(trade["symbol"], bool(trade["is_index"]), trade["expiry_date"])
                chain_cache[key] = summary
            if summary is None:
                trade["current_price"] = None
                trade["unrealized_pnl_inr"] = None
                trade["unrealized_pnl_pct"] = None
                continue
            legs = json.loads(trade["legs_json"])
            current_net = _price_legs(legs, summary)
            if current_net is None:
                trade["current_price"] = None
                trade["unrealized_pnl_inr"] = None
                trade["unrealized_pnl_pct"] = None
                continue
            entry = trade["entry_price"]
            if entry >= 0:
                pnl_pts = current_net - entry
            else:
                pnl_pts = abs(entry) - abs(current_net)
            pnl_inr = pnl_pts * trade["lot_size"] * trade["lots"]
            notional = trade.get("notional_inr") or 1
            pnl_pct = (pnl_inr / notional * 100) if notional else 0
            trade["current_price"] = round(current_net, 2)
            trade["unrealized_pnl_inr"] = round(pnl_inr, 2)
            trade["unrealized_pnl_pct"] = round(pnl_pct, 2)
        return trades

    async def count_open(self) -> int:
        async with self._connect() as db:
            db.row_factory = True
            async with db.execute("SELECT COUNT(*) as n FROM fo_trades WHERE status='open'") as c:
                return (await c.fetchone())["n"]

    async def get_open_trades(self) -> list:
        async with self._connect() as db:
            db.row_factory = True
            async with db.execute(
                "SELECT * FROM fo_trades WHERE status='open' ORDER BY opened_at DESC"
            ) as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_closed_trades(self, limit: int = 100) -> list:
        async with self._connect() as db:
            db.row_factory = True
            async with db.execute(
                "SELECT * FROM fo_trades WHERE status='closed' ORDER BY closed_at DESC LIMIT ?",
                (limit,),
            ) as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_summary(self) -> dict:
        async with self._connect() as db:
            db.row_factory = True
            async with db.execute("SELECT COUNT(*) as n FROM fo_trades") as c:
                total = (await c.fetchone())["n"]
            async with db.execute("SELECT COUNT(*) as n FROM fo_trades WHERE status='open'") as c:
                open_n = (await c.fetchone())["n"]
            async with db.execute("SELECT COUNT(*) as n FROM fo_trades WHERE status='closed'") as c:
                closed_n = (await c.fetchone())["n"]
            async with db.execute("SELECT COUNT(*) as n FROM fo_trades WHERE status='closed' AND pnl_inr>0") as c:
                wins = (await c.fetchone())["n"]
            async with db.execute("SELECT COALESCE(SUM(pnl_inr),0) as p FROM fo_trades WHERE status='closed'") as c:
                total_pnl = (await c.fetchone())["p"]
            async with db.execute("SELECT COALESCE(SUM(notional_inr),0) as d FROM fo_trades WHERE status='open'") as c:
                deployed = (await c.fetchone())["d"]
        return {
            "total": total, "open": open_n, "closed": closed_n, "wins": wins,
            "win_rate": round(wins / closed_n, 4) if closed_n else 0,
            "total_pnl": round(total_pnl, 2),
            "deployed": round(deployed, 2),
        }


def _price_legs(legs: list, chain_summary: dict) -> float | None:
    total = 0.0
    for leg in legs:
        row = next((s for s in chain_summary["strikes"] if s["strike"] == leg["strike"]), None)
        if row is None:
            return None
        ltp = row["ce_ltp"] if leg["type"] == "CE" else row["pe_ltp"]
        if ltp is None:
            return None
        sign = 1 if leg["action"] == "BUY" else -1
        total += sign * ltp
    return round(total, 2)
