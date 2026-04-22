"""F&O strategy builder — converts an R1 agent decision into concrete trade legs.

Input: strategy name + anchor strike + chain_summary
Output: legs list, net_premium, target, SL, risk_reward, direction, max_loss_inr.
Pure function — no I/O.

Supported strategies (per user spec):
  BUY_CE              bullish, long 1 call at strike
  BUY_PE              bearish, long 1 put at strike
  BULL_CALL_SPREAD    bullish, long ATM CE + short OTM CE (debit spread)
  BEAR_PUT_SPREAD     bearish, long ATM PE + short OTM PE (debit spread)
  SELL_STRANGLE       neutral, short OTM CE + short OTM PE (credit, undefined risk)
  IRON_CONDOR         neutral, SELL_STRANGLE + buy further-OTM CE/PE wings (credit, defined risk)
"""

from typing import Optional
from data.options_chain import nearest_strike, pick_strike, LOT_SIZES


OTM_STEPS = 2           # strikes away from anchor for OTM legs
WING_STEPS = 4          # strikes away for Iron Condor wings


def _leg(action: str, opt_type: str, strike_row: dict) -> dict:
    price = strike_row["ce_ltp"] if opt_type == "CE" else strike_row["pe_ltp"]
    return {
        "action": action,
        "type": opt_type,
        "strike": strike_row["strike"],
        "price": price,
    }


def _net_premium(legs: list) -> float:
    total = 0.0
    for leg in legs:
        sign = 1 if leg["action"] == "BUY" else -1
        total += sign * leg["price"]
    return round(total, 2)


def build_trade(
    symbol: str,
    strategy: str,
    anchor_strike: float,
    chain_summary: dict,
    confidence_pct: float,
) -> Optional[dict]:
    """Return a fully-specified trade dict, or None if legs can't be priced."""
    anchor = nearest_strike(chain_summary, anchor_strike)
    if anchor is None:
        return None

    legs: list = []
    direction = "neutral"
    max_loss_per_lot: float = 0.0
    max_profit_per_lot: float = 0.0

    if strategy == "BUY_CE":
        if anchor["ce_ltp"] <= 0:
            return None
        legs = [_leg("BUY", "CE", anchor)]
        direction = "bullish"

    elif strategy == "BUY_PE":
        if anchor["pe_ltp"] <= 0:
            return None
        legs = [_leg("BUY", "PE", anchor)]
        direction = "bearish"

    elif strategy == "BULL_CALL_SPREAD":
        short_leg = _find_by_offset(chain_summary, anchor, OTM_STEPS)
        if not short_leg or anchor["ce_ltp"] <= 0 or short_leg["ce_ltp"] <= 0:
            return None
        legs = [_leg("BUY", "CE", anchor), _leg("SELL", "CE", short_leg)]
        direction = "bullish"

    elif strategy == "BEAR_PUT_SPREAD":
        short_leg = _find_by_offset(chain_summary, anchor, -OTM_STEPS)
        if not short_leg or anchor["pe_ltp"] <= 0 or short_leg["pe_ltp"] <= 0:
            return None
        legs = [_leg("BUY", "PE", anchor), _leg("SELL", "PE", short_leg)]
        direction = "bearish"

    elif strategy == "SELL_STRANGLE":
        ce_leg = _find_by_offset(chain_summary, anchor, OTM_STEPS)
        pe_leg = _find_by_offset(chain_summary, anchor, -OTM_STEPS)
        if not ce_leg or not pe_leg or ce_leg["ce_ltp"] <= 0 or pe_leg["pe_ltp"] <= 0:
            return None
        legs = [_leg("SELL", "CE", ce_leg), _leg("SELL", "PE", pe_leg)]
        direction = "neutral"

    elif strategy == "IRON_CONDOR":
        ce_short = _find_by_offset(chain_summary, anchor, OTM_STEPS)
        ce_long = _find_by_offset(chain_summary, anchor, WING_STEPS)
        pe_short = _find_by_offset(chain_summary, anchor, -OTM_STEPS)
        pe_long = _find_by_offset(chain_summary, anchor, -WING_STEPS)
        if not all([ce_short, ce_long, pe_short, pe_long]):
            return None
        if ce_short["ce_ltp"] <= 0 or ce_long["ce_ltp"] <= 0 \
                or pe_short["pe_ltp"] <= 0 or pe_long["pe_ltp"] <= 0:
            return None
        legs = [
            _leg("SELL", "CE", ce_short), _leg("BUY", "CE", ce_long),
            _leg("SELL", "PE", pe_short), _leg("BUY", "PE", pe_long),
        ]
        direction = "neutral"

    else:
        return None

    net_prem = _net_premium(legs)
    target, stop_loss, max_loss_per_lot, max_profit_per_lot = _targets(strategy, legs, net_prem)

    lot_size = LOT_SIZES.get(symbol, 1)
    reward = abs(target - net_prem)
    risk = abs(stop_loss - net_prem)
    rr = round(reward / risk, 2) if risk > 0 else 0.0

    return {
        "symbol": symbol,
        "strategy": strategy,
        "direction": direction,
        "legs": legs,
        "anchor_strike": anchor["strike"],
        "net_premium": net_prem,           # per-lot, in premium points (₹ per unit)
        "target": target,
        "stop_loss": stop_loss,
        "risk_reward": rr,
        "max_loss_per_lot_pts": max_loss_per_lot,
        "max_profit_per_lot_pts": max_profit_per_lot,
        "lot_size": lot_size,
        "confidence_pct": confidence_pct,
    }


def _find_by_offset(summary: dict, anchor: dict, offset: int) -> Optional[dict]:
    """Find strike dict at (anchor index + offset). Anchor is a strike row, not ATM."""
    strikes = summary["strikes"]
    try:
        anchor_idx = next(i for i, s in enumerate(strikes) if s["strike"] == anchor["strike"])
    except StopIteration:
        return None
    target = anchor_idx + offset
    if target < 0 or target >= len(strikes):
        return None
    return strikes[target]


def _targets(strategy: str, legs: list, net_prem: float) -> tuple:
    """Returns (target, stop_loss, max_loss_pts, max_profit_pts) all per-lot in premium points."""
    if strategy in ("BUY_CE", "BUY_PE"):
        # Long single: 2x premium target, 0.5x SL
        debit = net_prem  # positive
        return round(debit * 2.0, 2), round(debit * 0.5, 2), debit, debit  # max loss = premium paid
    if strategy in ("BULL_CALL_SPREAD", "BEAR_PUT_SPREAD"):
        strike_diff = abs(legs[0]["strike"] - legs[1]["strike"])
        debit = net_prem
        max_profit = max(strike_diff - debit, 0.01)
        target = debit + max_profit * 0.7
        stop_loss = debit * 0.5
        return round(target, 2), round(stop_loss, 2), debit, max_profit
    if strategy == "SELL_STRANGLE":
        credit = abs(net_prem)  # net_prem is negative
        target = -credit * 0.5   # buy back at 50% of credit
        stop_loss = -credit * 2.0
        return round(target, 2), round(stop_loss, 2), credit * 2.0, credit  # undefined risk; cap at 2x
    if strategy == "IRON_CONDOR":
        credit = abs(net_prem)
        # Max loss = wing_width - credit; use short/long CE distance
        short_ce = next(l for l in legs if l["action"] == "SELL" and l["type"] == "CE")
        long_ce = next(l for l in legs if l["action"] == "BUY" and l["type"] == "CE")
        wing_width = abs(long_ce["strike"] - short_ce["strike"])
        max_loss = max(wing_width - credit, 0.01)
        target = -credit * 0.5
        stop_loss = -(credit + max_loss * 0.8)
        return round(target, 2), round(stop_loss, 2), max_loss, credit
    return 0.0, 0.0, 0.0, 0.0
