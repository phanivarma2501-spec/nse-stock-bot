"""
Smoke test for the Level 4 F&O agent pipeline.

Tests (no external API calls):
  1. All new modules import cleanly
  2. fo_strategy.build_trade produces legs for each of 6 strategies
  3. fo_kelly sizing respects min_confidence + max_lots + notional cap
  4. fo_calibration.calibrate round-trips
  5. fo_reasoning._parse_decision parses valid + invalid trailer blocks
  6. FOExecutor DB lifecycle (init / open / mark_and_close / summary) on local sqlite

Does NOT call DeepSeek, GDELT, or Kite — those require secrets and network.
"""
import os
import sys
import asyncio

os.environ.pop("TURSO_DATABASE_URL", None)
os.environ.pop("TURSO_AUTH_TOKEN", None)
os.environ.pop("RAILWAY_ENVIRONMENT", None)
os.environ.pop("PORT", None)
import turso_client  # noqa
turso_client._FALLBACK_URL = ""
turso_client._FALLBACK_TOKEN = ""

from loguru import logger
logger.remove()
logger.add(sys.stderr, level="INFO")


def _synth_summary(spot: float, expiry: str) -> dict:
    """Build a fake chain summary of the shape summarize_chain/kite_client return."""
    step = 50 if spot < 10000 else 100
    atm = round(spot / step) * step
    strikes = []
    for i in range(-5, 6):
        k = atm + step * i
        moneyness = k - spot
        ce_ltp = max(spot - k, 0) + max(80 - abs(moneyness) / 10, 8)
        pe_ltp = max(k - spot, 0) + max(80 - abs(moneyness) / 10, 8)
        strikes.append({
            "strike": k,
            "ce_ltp": round(ce_ltp, 2), "ce_oi": 100000 + abs(moneyness) * 20,
            "ce_iv": 15 + abs(moneyness) / spot * 40, "ce_volume": 5000,
            "pe_ltp": round(pe_ltp, 2), "pe_oi": 120000 + abs(moneyness) * 25,
            "pe_iv": 16 + abs(moneyness) / spot * 45, "pe_volume": 5500,
        })
    return {
        "spot": float(spot), "expiry": expiry, "expiries": [expiry],
        "atm_strike": atm, "atm_iv": 15.0, "strikes": strikes,
    }


def test_imports():
    print("\n[1] Imports")
    from data.options_chain import OptionsChainFetcher, summarize_chain, LOT_SIZES, INDICES, FNO_STOCKS
    from data.gdelt import get_news_context
    from data.global_cues import fetch_global_cues, format_cues_for_prompt
    from core.fo_calibration import calibrate, calculate_brier_score
    from core.fo_kelly import size_in_lots
    from core.fo_strategy import build_trade
    from core.fo_executor import FOExecutor
    from agents.fo_research import research_symbol
    from agents.fo_reasoning import reason_trade, _parse_decision, VALID_STRATEGIES
    from kite_client import KiteOptionsChainFetcher, implied_volatility
    import stock_bot, web_dashboard  # ensure they still import
    assert LOT_SIZES["NIFTY"] == 75 and LOT_SIZES["BANKNIFTY"] == 30
    print("  OK — all modules import, LOT_SIZES updated (NIFTY=75, BANKNIFTY=30)")


def test_strategy_build():
    print("\n[2] fo_strategy.build_trade — 6 strategies")
    from core.fo_strategy import build_trade
    summary = _synth_summary(24200, "28-Apr-2026")

    for strat in ("BUY_CE", "BUY_PE", "BULL_CALL_SPREAD", "BEAR_PUT_SPREAD",
                  "SELL_STRANGLE", "IRON_CONDOR"):
        trade = build_trade("NIFTY", strat, 24200, summary, confidence_pct=80)
        assert trade is not None, f"build_trade({strat}) returned None"
        assert trade["strategy"] == strat
        assert trade["legs"], f"no legs for {strat}"
        print(f"  {strat:20s} -> legs={len(trade['legs'])} net={trade['net_premium']} "
              f"t={trade['target']} sl={trade['stop_loss']} r:r={trade['risk_reward']}")
    print("  OK")


def test_kelly_sizing():
    print("\n[3] fo_kelly.size_in_lots")
    from core.fo_kelly import size_in_lots
    # Below threshold
    r = size_in_lots(60, 5000, 75, min_confidence=65)
    assert r["lots"] == 0
    # Exactly threshold -> 1 lot
    r = size_in_lots(65, 5000, 75, min_confidence=65)
    assert r["lots"] == 1
    # High confidence -> 2 lots
    r = size_in_lots(80, 5000, 75, min_confidence=65, max_lots=2)
    assert r["lots"] == 2
    # Notional cap kicks in
    r = size_in_lots(80, 40000, 75, max_lots=2, max_notional_inr=50000)
    assert r["lots"] == 1, f"expected 1, got {r}"
    # Credit strategy -> capped at 1 lot regardless
    r = size_in_lots(80, -6000, 75)
    assert r["lots"] == 1
    print("  OK — threshold + max_lots + notional cap + credit-cap all behave")


def test_calibration():
    print("\n[4] fo_calibration.calibrate")
    from core.fo_calibration import calibrate
    assert abs(calibrate(0.5) - 0.5) < 0.01, "50% should stay 50%"
    c80 = calibrate(0.80)
    assert 0.6 < c80 < 0.80, f"80% should compress, got {c80}"
    c20 = calibrate(0.20)
    assert 0.20 < c20 < 0.40
    print(f"  50%->{calibrate(0.5):.3f} (identity) 80%->{c80:.3f} (compressed) 20%->{c20:.3f} (compressed)")
    print("  OK")


def test_reasoning_parser():
    print("\n[5] fo_reasoning._parse_decision")
    from agents.fo_reasoning import _parse_decision

    # Valid YES
    text_yes = """... some reasoning ...
TRADE: YES
STRATEGY: BULL_CALL_SPREAD
STRIKE: 24200
EXPIRY: WEEKLY
CONFIDENCE: 75"""
    d = _parse_decision(text_yes)
    assert d and d["trade"] == "YES" and d["strategy"] == "BULL_CALL_SPREAD" \
        and d["strike"] == 24200.0 and d["expiry"] == "WEEKLY" and d["confidence"] == 75
    print(f"  valid YES   -> {d}")

    # Valid NO
    text_no = "(reasoning) ...\nTRADE: NO\nCONFIDENCE: 40"
    d = _parse_decision(text_no)
    assert d and d["trade"] == "NO" and d["confidence"] == 40
    print(f"  valid NO    -> {d}")

    # Invalid strategy -> rejected
    text_bad_strat = "TRADE: YES\nSTRATEGY: MAGIC_SPREAD\nSTRIKE: 24200\nEXPIRY: WEEKLY\nCONFIDENCE: 80"
    d = _parse_decision(text_bad_strat)
    assert d is None, f"bad strategy should be rejected, got {d}"
    print(f"  bad strat   -> rejected (None)")

    # Missing trailer -> None
    d = _parse_decision("just some reasoning without trailer")
    assert d is None
    print(f"  no trailer  -> None")
    print("  OK")


async def test_executor_lifecycle():
    print("\n[6] FOExecutor DB lifecycle")
    from core.fo_executor import FOExecutor
    from core.fo_strategy import build_trade

    path = "data/_l4_smoke.db"
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    os.makedirs("data", exist_ok=True)

    exe = FOExecutor(path)
    await exe.init()

    summary = _synth_summary(24200, "11-Jul-2099")
    trade = build_trade("NIFTY", "BUY_CE", 24200, summary, confidence_pct=80)
    trade.update({
        "is_index": True, "expiry_date": "11-Jul-2099", "expiry_type": "WEEKLY",
        "lots": 1, "notional_inr": trade["net_premium"] * 75,
        "confidence": 80, "edge": 0.2, "reasoning": "test",
        "equity_signal": "BUY", "atm_iv": 15.0, "iv_regime": "low",
        "pcr": 1.2, "max_pain": 24300,
    })
    tid = await exe.open_trade(trade)
    assert tid, "open_trade should return an id"
    print(f"  opened: {tid[:8]}")

    # Duplicate skip
    tid2 = await exe.open_trade(trade)
    assert tid2 is None, "duplicate should skip"
    print("  duplicate skipped")

    # Mark-and-close with fake fetch that bumps CE to trigger target
    async def fake_fetch(symbol, is_index, expiry):
        s = _synth_summary(24400, expiry)
        for row in s["strikes"]:
            if row["strike"] == 24200:
                row["ce_ltp"] = trade["net_premium"] * 2.5  # above target
        return s

    closed = await exe.mark_and_close(fake_fetch)
    assert closed == 1
    print(f"  mark_and_close closed {closed} (target hit)")

    # Expiry path
    past_trade = dict(trade)
    past_trade["expiry_date"] = "01-Jan-2020"
    tid3 = await exe.open_trade(past_trade)
    assert tid3
    closed = await exe.mark_and_close(fake_fetch)
    assert closed == 1

    # Scan log
    await exe.log_scan(symbols=22, signals=2, placed=1, errors=0, notes="test")

    summary_db = await exe.get_summary()
    print(f"  summary: {summary_db}")
    assert summary_db["total"] == 2 and summary_db["closed"] == 2

    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    print("  OK")


async def main():
    test_imports()
    test_strategy_build()
    test_kelly_sizing()
    test_calibration()
    test_reasoning_parser()
    await test_executor_lifecycle()
    print("\n=== Level 4 smoke test passed ===")


if __name__ == "__main__":
    asyncio.run(main())
