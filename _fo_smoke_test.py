"""
Smoke test for F&O module. Run locally before pushing.

Tests:
  1. All modules import cleanly
  2. summarize_chain handles a synthetic NSE-shaped payload
  3. PCR / max-pain / IV regime / iv_rank compute
  4. recommend() fires for each of the 5 mapped scenarios
  5. FOStorage init + open + mark-to-market + close + expire lifecycle (local sqlite)
  6. (Optional) Real NSE chain fetch — may fail from non-IN IPs; failure logged, test continues.
"""

import os
import asyncio
import sys

# Force local sqlite (no Turso) for test.
# turso_client has hardcoded _FALLBACK_URL + _FALLBACK_TOKEN, so popping env vars
# alone is NOT enough — the client will still connect to production Turso. We
# must also blank those module-level fallbacks before importing anything else.
os.environ.pop("TURSO_DATABASE_URL", None)
os.environ.pop("TURSO_AUTH_TOKEN", None)
os.environ.pop("RAILWAY_ENVIRONMENT", None)
os.environ.pop("PORT", None)
import turso_client  # noqa: E402
turso_client._FALLBACK_URL = ""
turso_client._FALLBACK_TOKEN = ""

from loguru import logger
logger.remove()
logger.add(sys.stderr, level="INFO")

from options_chain import (
    OptionsChainFetcher, summarize_chain, compute_pcr, compute_max_pain,
    iv_regime, iv_rank_in_chain, pick_strike, INDICES, FNO_STOCKS, LOT_SIZES,
)
from options_strategy import recommend
from paper_trades_fo import FOStorage


def _synth_chain(spot: float, expiry: str) -> dict:
    """Build an NSE-shaped raw response with strikes at 50-point intervals around `spot`."""
    strike_step = 50 if spot < 10000 else 100
    atm = round(spot / strike_step) * strike_step
    strikes_arr = [atm + strike_step * i for i in range(-5, 6)]
    data = []
    for k in strikes_arr:
        moneyness = k - spot
        # synthetic IV smile: higher away from ATM
        iv_ce = 15 + abs(moneyness) / spot * 40
        iv_pe = 16 + abs(moneyness) / spot * 45
        ce_ltp = max(spot - k, 0) + max(50 - abs(moneyness) / 10, 5)
        pe_ltp = max(k - spot, 0) + max(50 - abs(moneyness) / 10, 5)
        data.append({
            "strikePrice": k,
            "expiryDate": expiry,
            "CE": {
                "lastPrice": round(ce_ltp, 2),
                "openInterest": 100000 + abs(moneyness) * 20,
                "impliedVolatility": round(iv_ce, 2),
                "totalTradedVolume": 5000,
            },
            "PE": {
                "lastPrice": round(pe_ltp, 2),
                "openInterest": 120000 + abs(moneyness) * 25,
                "impliedVolatility": round(iv_pe, 2),
                "totalTradedVolume": 5500,
            },
        })
    return {"records": {"data": data, "expiryDates": [expiry], "underlyingValue": spot}}


def test_summarize_and_analytics():
    print("\n[1] summarize_chain + analytics")
    raw = _synth_chain(24200, "11-Jul-2024")
    summary = summarize_chain(raw)
    assert summary is not None, "summary should not be None"
    assert summary["spot"] == 24200
    assert summary["atm_strike"] == 24200
    assert len(summary["strikes"]) == 11
    pcr = compute_pcr(summary)
    mp = compute_max_pain(summary)
    ivr = iv_rank_in_chain(summary)
    regime_idx = iv_regime(summary["atm_iv"], True)
    print(f"  spot={summary['spot']} atm={summary['atm_strike']} atm_iv={summary['atm_iv']}")
    print(f"  pcr={pcr}  max_pain={mp}  iv_rank_in_chain={ivr}  regime(index)={regime_idx}")
    assert pcr > 0 and mp in [s["strike"] for s in summary["strikes"]]
    print("  OK")


def test_strategy_mapping():
    print("\n[2] recommend() — each mapped scenario")

    # Low IV index chain (manually override atm_iv)
    low_iv_summary = summarize_chain(_synth_chain(24200, "11-Jul-2024"))
    low_iv_summary["atm_iv"] = 10.0  # force low regime for index

    high_iv_summary = summarize_chain(_synth_chain(24200, "11-Jul-2024"))
    high_iv_summary["atm_iv"] = 25.0  # force high regime for index

    # Strong BUY + low IV -> BUY_CALL
    rec = recommend("NIFTY", {"signal": "BUY", "strength": "STRONG", "confidence": 0.8}, low_iv_summary, True)
    assert rec and rec["strategy"] == "BUY_CALL", f"expected BUY_CALL got {rec}"
    print(f"  STRONG BUY + low  -> {rec['strategy']}  net={rec['net_premium']}  t={rec['target']} sl={rec['stop_loss']} r:r={rec['risk_reward']}")

    # Strong SELL + low IV -> BUY_PUT
    rec = recommend("NIFTY", {"signal": "SELL", "strength": "STRONG", "confidence": 0.75}, low_iv_summary, True)
    assert rec and rec["strategy"] == "BUY_PUT"
    print(f"  STRONG SELL+ low  -> {rec['strategy']}  net={rec['net_premium']}")

    # HOLD + high IV -> SELL_STRANGLE
    rec = recommend("NIFTY", {"signal": "HOLD", "strength": "WEAK", "confidence": 0.4}, high_iv_summary, True)
    assert rec and rec["strategy"] == "SELL_STRANGLE"
    assert rec["net_premium"] < 0, "strangle should be net credit"
    print(f"  HOLD + high       -> {rec['strategy']}  credit={rec['net_premium']}  t={rec['target']} sl={rec['stop_loss']}")

    # Strong BUY + high IV -> BULL_CALL_SPREAD
    rec = recommend("NIFTY", {"signal": "BUY", "strength": "STRONG", "confidence": 0.8}, high_iv_summary, True)
    assert rec and rec["strategy"] == "BULL_CALL_SPREAD"
    print(f"  STRONG BUY + high -> {rec['strategy']}  debit={rec['net_premium']}  t={rec['target']} r:r={rec['risk_reward']}")

    # Strong SELL + high IV -> BEAR_PUT_SPREAD
    rec = recommend("NIFTY", {"signal": "SELL", "strength": "STRONG", "confidence": 0.75}, high_iv_summary, True)
    assert rec and rec["strategy"] == "BEAR_PUT_SPREAD"
    print(f"  STRONG SELL+ high -> {rec['strategy']}  debit={rec['net_premium']}  t={rec['target']}")

    # Weak BUY -> no trade
    rec = recommend("NIFTY", {"signal": "BUY", "strength": "WEAK", "confidence": 0.5}, low_iv_summary, True)
    assert rec is None, "weak signal should produce no trade"
    print("  WEAK BUY          -> no trade  [ok]")

    print("  OK")


async def test_storage_lifecycle():
    print("\n[3] FOStorage open + mark-to-market + close (local sqlite)")
    db_path = "data/_fo_smoke_test.db"
    # Clean any prior run
    try:
        os.remove(db_path)
    except FileNotFoundError:
        pass
    os.makedirs("data", exist_ok=True)

    store = FOStorage(db_path)
    await store.init()

    raw = _synth_chain(24200, "11-Jul-2099")  # far-future expiry so we don't auto-expire
    summary = summarize_chain(raw)
    summary["atm_iv"] = 10.0  # force low regime
    rec = recommend("NIFTY", {"signal": "BUY", "strength": "STRONG", "confidence": 0.8}, summary, True)
    assert rec is not None
    rec["is_index"] = True

    tid = await store.open_trade(rec)
    assert tid, "open_trade should return an id"
    print(f"  opened trade id={tid[:8]}... strategy={rec['strategy']}")

    # Duplicate skip
    tid2 = await store.open_trade(rec)
    assert tid2 is None, "duplicate should be skipped"
    print("  duplicate open skipped [ok]")

    # Mark to market with a pumped-up chain (CE LTP doubled) -> target hit
    async def fake_fetch(symbol, is_index, expiry):
        raw2 = _synth_chain(24400, expiry or "11-Jul-2099")
        # jack up CE to force target hit on the long call
        for d in raw2["records"]["data"]:
            if d["strikePrice"] == 24200:
                d["CE"]["lastPrice"] = rec["net_premium"] * 2.5
        return summarize_chain(raw2, expiry or "11-Jul-2099")

    closed = await store.mark_and_close(None, None, fake_fetch)
    print(f"  mark_and_close returned closed={closed}")
    assert closed == 1, "trade should close on target hit"

    closed_trades = await store.get_closed_trades()
    assert closed_trades and closed_trades[0]["close_reason"] == "TARGET"
    print(f"  closed with reason={closed_trades[0]['close_reason']} pnl=Rs {closed_trades[0]['pnl_inr']:+,.0f}")

    # Expiry path: open again with a past expiry, mark_and_close should close as EXPIRED
    rec_expired = dict(rec)
    rec_expired["expiry"] = "01-Jan-2020"
    tid3 = await store.open_trade(rec_expired)
    assert tid3
    closed2 = await store.mark_and_close(None, None, fake_fetch)
    assert closed2 == 1
    closed_trades = await store.get_closed_trades()
    reasons = [t["close_reason"] for t in closed_trades]
    assert "EXPIRED" in reasons
    print(f"  expired trade closed with reason=EXPIRED [ok]")

    summary_db = await store.get_summary()
    print(f"  summary: {summary_db}")

    # Cleanup
    try:
        os.remove(db_path)
    except FileNotFoundError:
        pass
    print("  OK")


async def test_real_nse_fetch():
    print("\n[4] Real NSE chain fetch (may fail on non-IN IP)")
    f = OptionsChainFetcher()
    try:
        raw = await f.fetch_chain("NIFTY", is_index=True)
        if not raw:
            print("  chain fetch returned None (blocked or network issue) — OK, graceful failure")
            return
        summary = summarize_chain(raw)
        if not summary:
            print("  chain was non-empty but summary failed (shape changed?) — check raw")
            return
        print(f"  NIFTY spot={summary['spot']} atm={summary['atm_strike']} atm_iv={summary['atm_iv']:.2f}% "
              f"expiry={summary['expiry']} strikes={len(summary['strikes'])}")
        print(f"  PCR={compute_pcr(summary)}  max_pain={compute_max_pain(summary)}  "
              f"iv_rank={iv_rank_in_chain(summary)}")
    finally:
        await f.close()


async def main():
    test_summarize_and_analytics()
    test_strategy_mapping()
    await test_storage_lifecycle()
    await test_real_nse_fetch()
    print("\n=== All tests passed ===")


if __name__ == "__main__":
    asyncio.run(main())
