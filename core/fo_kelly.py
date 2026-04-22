"""Lot-based sizing for F&O paper trading.

Adapted from phani-market-v2/core/kelly.py. The binary YES/NO edge framing
doesn't map cleanly to options (continuous payoff) so we simplify: the agent
gives confidence 0-100; this module maps confidence + premium cost to a lot
count, capped at FO_MAX_LOTS.

Rules:
  - confidence < min_confidence   -> 0 lots (no trade)
  - min_confidence <= c < 75      -> 1 lot
  - c >= 75                       -> 2 lots (up to FO_MAX_LOTS)
  - cost (premium × lot_size × lots) must stay under max_notional_inr
"""

from typing import Optional


def size_in_lots(
    confidence_pct: float,
    net_premium_per_lot: float,
    lot_size: int,
    min_confidence: int = 65,
    max_lots: int = 2,
    max_notional_inr: float = 50000.0,
) -> dict:
    """Returns lot count + notional cost.

    confidence_pct: 0-100
    net_premium_per_lot: signed INR (debit +, credit -) per lot based on leg prices
    lot_size: shares/units per lot
    """
    if confidence_pct < min_confidence:
        return {"lots": 0, "reason": f"confidence {confidence_pct} < min {min_confidence}"}

    lots = 2 if confidence_pct >= 75 else 1
    lots = min(lots, max_lots)

    # For debit strategies, premium is money out; for credit strategies, we
    # collect premium but risk exceeds it. Use abs() for notional cap.
    notional = abs(net_premium_per_lot) * lots
    if net_premium_per_lot < 0:
        # Credit strategy — notional risk is larger; cap lots at 1 regardless
        lots = min(lots, 1)
        notional = abs(net_premium_per_lot) * lots

    while lots > 1 and notional > max_notional_inr:
        lots -= 1
        notional = abs(net_premium_per_lot) * lots

    if lots == 0:
        return {"lots": 0, "reason": "notional cap"}

    return {
        "lots": lots,
        "notional_inr": round(notional, 2),
        "lot_size": lot_size,
        "confidence_pct": confidence_pct,
    }
