"""
F&O strategy recommender.

Maps (equity signal, IV regime) → option strategy with legs, entry, target, SL, R:R.
Pure functions — no network/DB I/O.

Mapping (from user spec):
  Strong BUY  + Low IV    → Buy Call (long CE, ATM)
  Strong SELL + Low IV    → Buy Put  (long PE, ATM)
  Neutral     + High IV   → Sell Strangle (short OTM CE + short OTM PE)
  Strong BUY  + High IV   → Bull Call Spread (long ATM CE + short OTM CE)
  Strong SELL + High IV   → Bear Put Spread  (long ATM PE + short OTM PE)
  Else                    → no trade
"""

from typing import Optional
from options_chain import pick_strike, iv_regime, LOT_SIZES


OTM_STEPS = 2  # how many strikes away from ATM for OTM legs


def _leg(action: str, opt_type: str, strike_row: dict, lots: int) -> dict:
    price = strike_row["ce_ltp"] if opt_type == "CE" else strike_row["pe_ltp"]
    return {
        "action": action,          # BUY / SELL
        "type": opt_type,          # CE / PE
        "strike": strike_row["strike"],
        "price": price,
        "lots": lots,
    }


def _net_premium(legs: list) -> float:
    """Net debit (+) or credit (-) per lot, in points."""
    total = 0.0
    for leg in legs:
        sign = 1 if leg["action"] == "BUY" else -1
        total += sign * leg["price"]
    return round(total, 2)


def recommend(
    symbol: str,
    equity_signal: dict,
    chain_summary: dict,
    is_index: bool,
    lots: int = 1,
) -> Optional[dict]:
    """Produce a strategy recommendation, or None if no trade fits.

    equity_signal: dict with keys signal (BUY/SELL/HOLD), strength, confidence.
    chain_summary: output of options_chain.summarize_chain().
    """
    sig = (equity_signal or {}).get("signal", "HOLD")
    strength = (equity_signal or {}).get("strength", "WEAK")
    conf = (equity_signal or {}).get("confidence", 0) or 0
    atm_iv = chain_summary["atm_iv"]
    regime = iv_regime(atm_iv, is_index)

    is_strong = strength == "STRONG"
    is_hold = sig == "HOLD"

    strategy = None
    legs: list = []
    direction = "neutral"

    atm_row = next(
        (s for s in chain_summary["strikes"] if s["strike"] == chain_summary["atm_strike"]),
        None,
    )
    if atm_row is None:
        return None

    if is_strong and sig == "BUY" and regime == "low":
        strategy = "BUY_CALL"
        direction = "bullish"
        legs = [_leg("BUY", "CE", atm_row, lots)]

    elif is_strong and sig == "SELL" and regime == "low":
        strategy = "BUY_PUT"
        direction = "bearish"
        legs = [_leg("BUY", "PE", atm_row, lots)]

    elif is_hold and regime == "high":
        otm_ce = pick_strike(chain_summary, +OTM_STEPS)
        otm_pe = pick_strike(chain_summary, -OTM_STEPS)
        if not otm_ce or not otm_pe or otm_ce["ce_ltp"] <= 0 or otm_pe["pe_ltp"] <= 0:
            return None
        strategy = "SELL_STRANGLE"
        direction = "neutral"
        legs = [_leg("SELL", "CE", otm_ce, lots), _leg("SELL", "PE", otm_pe, lots)]

    elif is_strong and sig == "BUY" and regime == "high":
        otm_ce = pick_strike(chain_summary, +OTM_STEPS)
        if not otm_ce or otm_ce["ce_ltp"] <= 0 or atm_row["ce_ltp"] <= 0:
            return None
        strategy = "BULL_CALL_SPREAD"
        direction = "bullish"
        legs = [_leg("BUY", "CE", atm_row, lots), _leg("SELL", "CE", otm_ce, lots)]

    elif is_strong and sig == "SELL" and regime == "high":
        otm_pe = pick_strike(chain_summary, -OTM_STEPS)
        if not otm_pe or otm_pe["pe_ltp"] <= 0 or atm_row["pe_ltp"] <= 0:
            return None
        strategy = "BEAR_PUT_SPREAD"
        direction = "bearish"
        legs = [_leg("BUY", "PE", atm_row, lots), _leg("SELL", "PE", otm_pe, lots)]

    else:
        return None

    net_prem = _net_premium(legs)
    lot_size = LOT_SIZES.get(symbol, 1)
    size_inr = abs(net_prem) * lot_size * lots

    # Target / SL per strategy (on the *net premium* quoted in points).
    if strategy in ("BUY_CALL", "BUY_PUT"):
        # Long single-leg: target 2x premium paid, SL 50% of premium paid.
        target = round(net_prem * 2.0, 2)
        stop_loss = round(net_prem * 0.5, 2)
        max_loss_inr = net_prem * lot_size * lots  # premium paid
    elif strategy in ("BULL_CALL_SPREAD", "BEAR_PUT_SPREAD"):
        # Debit spread: max profit ≈ (strike_diff - net_debit); target 70% of max profit.
        strike_diff = abs(legs[0]["strike"] - legs[1]["strike"])
        max_profit = max(strike_diff - net_prem, 0.01)
        target = round(net_prem + max_profit * 0.7, 2)
        stop_loss = round(net_prem * 0.5, 2)
        max_loss_inr = net_prem * lot_size * lots
    else:  # SELL_STRANGLE — net credit (negative net_prem)
        credit = abs(net_prem)
        # Close at 50% of credit captured → target = -credit*0.5 (i.e. buy back cheaper).
        target = round(-credit * 0.5, 2)
        # SL at 2x credit (premium doubles against us).
        stop_loss = round(-credit * 2.0, 2)
        # Undefined risk — size_inr used as notional; show 2x credit as the managed loss.
        max_loss_inr = credit * 2.0 * lot_size * lots

    # R:R: reward vs risk, both in premium points.
    reward = abs(target - net_prem)
    risk = abs(stop_loss - net_prem)
    rr = round(reward / risk, 2) if risk > 0 else 0.0

    return {
        "symbol": symbol,
        "strategy": strategy,
        "direction": direction,
        "expiry": chain_summary["expiry"],
        "legs": legs,
        "spot": chain_summary["spot"],
        "atm_strike": chain_summary["atm_strike"],
        "atm_iv": round(atm_iv, 2),
        "iv_regime": regime,
        "net_premium": net_prem,
        "target": target,
        "stop_loss": stop_loss,
        "risk_reward": rr,
        "lot_size": lot_size,
        "lots": lots,
        "size_inr": round(size_inr, 2),
        "max_loss_inr": round(max_loss_inr, 2),
        "confidence": conf,
        "equity_signal": sig,
        "equity_strength": strength,
    }
