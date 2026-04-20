"""
F&O paper-trade executor.

Stores F&O trades in Turso (or aiosqlite fallback) in an `fno_trades` table.
Marks positions to market by re-fetching the option chain and recomputing
net premium at current strike LTPs. Auto-expires trades past their expiry date.
"""

import json
import uuid
from datetime import datetime
from loguru import logger
from turso_client import connect


class FOStorage:
    """DB + lifecycle for F&O paper trades. Mirrors the equity Storage class shape."""

    def __init__(self, db_path: str):
        self._db_path = db_path

    def _connect(self):
        return connect(self._db_path)

    async def init(self):
        async with self._connect() as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS fno_trades (
                    id TEXT PRIMARY KEY,
                    symbol TEXT,
                    is_index INTEGER,
                    strategy TEXT,
                    direction TEXT,
                    expiry TEXT,
                    legs_json TEXT,
                    spot_at_entry REAL,
                    atm_strike REAL,
                    atm_iv REAL,
                    iv_regime TEXT,
                    net_premium REAL,
                    target REAL,
                    stop_loss REAL,
                    risk_reward REAL,
                    lot_size INTEGER,
                    lots INTEGER,
                    size_inr REAL,
                    max_loss_inr REAL,
                    equity_signal TEXT,
                    equity_strength TEXT,
                    signal_confidence REAL,
                    status TEXT DEFAULT 'open',
                    current_premium REAL,
                    pnl_inr REAL,
                    pnl_pct REAL,
                    entered_at TEXT,
                    exited_at TEXT,
                    close_reason TEXT
                )
            """)
            await db.commit()

    async def open_trade(self, rec: dict) -> str | None:
        """Insert a new F&O trade. Skips if an open trade exists for the same
        symbol + strategy + expiry (to avoid stacking on repeat scans)."""
        async with self._connect() as db:
            async with db.execute(
                """SELECT COUNT(*) FROM fno_trades
                   WHERE symbol=? AND strategy=? AND expiry=? AND status='open'""",
                (rec["symbol"], rec["strategy"], rec["expiry"]),
            ) as c:
                row = await c.fetchone()
                if row and row[0] > 0:
                    return None

            tid = str(uuid.uuid4())
            await db.execute(
                """INSERT INTO fno_trades VALUES
                   (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    tid, rec["symbol"], 1 if rec.get("is_index") else 0,
                    rec["strategy"], rec["direction"], rec["expiry"],
                    json.dumps(rec["legs"]),
                    rec["spot"], rec["atm_strike"], rec["atm_iv"], rec["iv_regime"],
                    rec["net_premium"], rec["target"], rec["stop_loss"],
                    rec["risk_reward"], rec["lot_size"], rec["lots"],
                    rec["size_inr"], rec["max_loss_inr"],
                    rec["equity_signal"], rec["equity_strength"], rec["confidence"],
                    "open", None, None, None,
                    datetime.now().isoformat(), None, None,
                ),
            )
            await db.commit()
            logger.info(
                f"FO PAPER TRADE OPENED: {rec['strategy']} {rec['symbol']} "
                f"exp={rec['expiry']} net={rec['net_premium']} "
                f"target={rec['target']} SL={rec['stop_loss']} "
                f"size=Rs {rec['size_inr']:,.0f}"
            )
            return tid

    async def mark_and_close(self, chain_fetcher, summarize, fetch_chain_for) -> int:
        """For each open trade:
          1. Re-fetch chain for its symbol + expiry.
          2. Price each leg at current LTP → current_net.
          3. If target/SL breached OR expiry passed → close.
        Returns number closed.

        fetch_chain_for(symbol, is_index, expiry) -> chain_summary | None
          (Passed in so this class doesn't import OptionsChainFetcher directly.)
        """
        trades = await self.get_open_trades()
        if not trades:
            return 0

        today = datetime.now().date()
        closed = 0
        # Cache chain summary per (symbol, expiry) — same trade may appear for multiple legs pricing.
        chain_cache: dict = {}

        for trade in trades:
            symbol = trade["symbol"]
            is_index = bool(trade["is_index"])
            expiry = trade["expiry"]

            # Expiry handling. Chain uses "11-Jul-2024" format.
            try:
                exp_date = datetime.strptime(expiry, "%d-%b-%Y").date()
            except ValueError:
                exp_date = None

            expired = exp_date is not None and exp_date < today

            key = (symbol, expiry)
            summary = chain_cache.get(key)
            if summary is None and not expired:
                summary = await fetch_chain_for(symbol, is_index, expiry)
                chain_cache[key] = summary

            if expired:
                await self._close(trade, current_net=None, reason="EXPIRED")
                closed += 1
                continue

            if summary is None:
                continue  # can't mark-to-market; leave open

            legs = json.loads(trade["legs_json"])
            current_net = _price_legs(legs, summary)
            if current_net is None:
                continue

            net_entry = trade["net_premium"]
            target = trade["target"]
            sl = trade["stop_loss"]

            # Determine direction of each threshold. For debits (net_entry>0),
            # target > entry (we profit as premium rises). For credits (net_entry<0),
            # target < entry (premium becomes less negative → profit).
            is_debit = net_entry >= 0
            if is_debit:
                hit_target = current_net >= target
                hit_sl = current_net <= sl
            else:
                hit_target = current_net >= target   # target is closer to 0 than entry
                hit_sl = current_net <= sl          # sl is more negative than entry

            if hit_target or hit_sl:
                await self._close(
                    trade,
                    current_net=current_net,
                    reason="TARGET" if hit_target else "STOP_LOSS",
                )
                closed += 1

        return closed

    async def _close(self, trade: dict, current_net: float | None, reason: str):
        """Close a trade. If current_net is None (EXPIRED w/o data), assume worst case."""
        net_entry = trade["net_premium"]
        lot_size = trade["lot_size"]
        lots = trade["lots"]

        if current_net is None:
            # Expired with no pricing — assume options expired worthless.
            current_net = 0.0

        # For debits: pnl = (current - entry) * lot * lots
        # For credits (entry<0): pnl = (entry - current) * lot * lots (flipping sign)
        if net_entry >= 0:
            pnl_points = current_net - net_entry
        else:
            pnl_points = abs(net_entry) - abs(current_net)
        pnl_inr = pnl_points * lot_size * lots
        pnl_pct = (pnl_inr / trade["size_inr"] * 100) if trade["size_inr"] else 0

        async with self._connect() as db:
            await db.execute(
                """UPDATE fno_trades SET
                    status='closed', current_premium=?, pnl_inr=?, pnl_pct=?,
                    exited_at=?, close_reason=?
                   WHERE id=?""",
                (
                    round(current_net, 2), round(pnl_inr, 2), round(pnl_pct, 2),
                    datetime.now().isoformat(), reason, trade["id"],
                ),
            )
            await db.commit()

        logger.info(
            f"FO PAPER TRADE CLOSED [{reason}]: {trade['strategy']} {trade['symbol']} "
            f"entry={net_entry} -> exit={current_net:.2f} | P&L: Rs {pnl_inr:+,.0f} ({pnl_pct:+.1f}%)"
        )

    async def mark_open_pnl(self, fetch_chain_for) -> list:
        """Return open trades with live current_premium + unrealized P&L annotated."""
        trades = await self.get_open_trades()
        chain_cache: dict = {}
        for trade in trades:
            key = (trade["symbol"], trade["expiry"])
            summary = chain_cache.get(key)
            if summary is None:
                summary = await fetch_chain_for(
                    trade["symbol"], bool(trade["is_index"]), trade["expiry"]
                )
                chain_cache[key] = summary
            if summary is None:
                trade["current_premium"] = None
                trade["unrealized_pnl_inr"] = None
                trade["unrealized_pnl_pct"] = None
                continue
            legs = json.loads(trade["legs_json"])
            current_net = _price_legs(legs, summary)
            if current_net is None:
                trade["current_premium"] = None
                trade["unrealized_pnl_inr"] = None
                trade["unrealized_pnl_pct"] = None
                continue
            net_entry = trade["net_premium"]
            if net_entry >= 0:
                pnl_points = current_net - net_entry
            else:
                pnl_points = abs(net_entry) - abs(current_net)
            pnl_inr = pnl_points * trade["lot_size"] * trade["lots"]
            pnl_pct = (pnl_inr / trade["size_inr"] * 100) if trade["size_inr"] else 0
            trade["current_premium"] = round(current_net, 2)
            trade["unrealized_pnl_inr"] = round(pnl_inr, 2)
            trade["unrealized_pnl_pct"] = round(pnl_pct, 2)
        return trades

    async def get_open_trades(self) -> list:
        async with self._connect() as db:
            db.row_factory = True
            async with db.execute(
                "SELECT * FROM fno_trades WHERE status='open' ORDER BY entered_at DESC"
            ) as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_closed_trades(self, limit: int = 50) -> list:
        async with self._connect() as db:
            db.row_factory = True
            async with db.execute(
                "SELECT * FROM fno_trades WHERE status='closed' ORDER BY exited_at DESC LIMIT ?",
                (limit,),
            ) as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_summary(self) -> dict:
        async with self._connect() as db:
            db.row_factory = True
            async with db.execute("SELECT COUNT(*) as n FROM fno_trades") as c:
                total = (await c.fetchone())["n"]
            async with db.execute(
                "SELECT COUNT(*) as n FROM fno_trades WHERE status='open'"
            ) as c:
                open_n = (await c.fetchone())["n"]
            async with db.execute(
                "SELECT COUNT(*) as n FROM fno_trades WHERE status='closed'"
            ) as c:
                closed_n = (await c.fetchone())["n"]
            async with db.execute(
                "SELECT COUNT(*) as n FROM fno_trades WHERE status='closed' AND pnl_inr>0"
            ) as c:
                wins = (await c.fetchone())["n"]
            async with db.execute(
                "SELECT COALESCE(SUM(pnl_inr),0) as p FROM fno_trades WHERE status='closed'"
            ) as c:
                total_pnl = (await c.fetchone())["p"]
            async with db.execute(
                "SELECT COALESCE(SUM(size_inr),0) as d FROM fno_trades WHERE status='open'"
            ) as c:
                deployed = (await c.fetchone())["d"]
        win_rate = wins / closed_n if closed_n else 0
        return {
            "total": total,
            "open": open_n,
            "closed": closed_n,
            "wins": wins,
            "win_rate": round(win_rate, 4),
            "total_pnl": round(total_pnl, 2),
            "deployed": round(deployed, 2),
        }


def _price_legs(legs: list, chain_summary: dict) -> float | None:
    """Compute net premium for given legs against current chain. Returns None if any leg missing."""
    total = 0.0
    for leg in legs:
        row = next(
            (s for s in chain_summary["strikes"] if s["strike"] == leg["strike"]),
            None,
        )
        if row is None:
            return None
        ltp = row["ce_ltp"] if leg["type"] == "CE" else row["pe_ltp"]
        if ltp is None:
            return None
        sign = 1 if leg["action"] == "BUY" else -1
        total += sign * ltp
    return round(total, 2)
