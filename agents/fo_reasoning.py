"""F&O reasoning agent — DeepSeek R1.

Takes equity signal + chain analytics + research summary and emits a structured
trade decision. Parses a trailer block per user spec:

    TRADE: YES/NO
    STRATEGY: BUY_CE | BUY_PE | BULL_CALL_SPREAD | BEAR_PUT_SPREAD | SELL_STRANGLE | IRON_CONDOR
    STRIKE: <price>
    EXPIRY: WEEKLY | MONTHLY
    CONFIDENCE: <0-100>

Returns tuple (decision_dict_or_None, full_reasoning_text).
Mirrors phani-market-v2/agents/reasoning.py extraction pattern.
"""

import os
import re
import asyncio
import requests
from typing import Optional, Tuple
from loguru import logger


DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1"
REASONING_MODEL = "deepseek-reasoner"  # DeepSeek R1


VALID_STRATEGIES = {
    "BUY_CE", "BUY_PE",
    "BULL_CALL_SPREAD", "BEAR_PUT_SPREAD",
    "SELL_STRANGLE", "IRON_CONDOR",
}
VALID_EXPIRIES = {"WEEKLY", "MONTHLY"}


async def reason_trade(
    symbol: str,
    is_index: bool,
    equity_signal: dict,
    chain_summary: dict,
    research: dict,
    pcr: float,
    max_pain: float,
    iv_rank: float,
    days_to_expiry: int,
) -> Tuple[Optional[dict], str]:
    """Call R1, parse trailer, return structured decision + raw text."""
    return await asyncio.to_thread(
        _reason_sync,
        symbol, is_index, equity_signal, chain_summary, research,
        pcr, max_pain, iv_rank, days_to_expiry,
    )


def _reason_sync(
    symbol, is_index, equity_signal, chain_summary, research,
    pcr, max_pain, iv_rank, days_to_expiry,
) -> Tuple[Optional[dict], str]:
    strategies_list = ", ".join(sorted(VALID_STRATEGIES))

    prompt = f"""You are an expert Indian F&O options trader. Decide whether to open a paper trade on {symbol}.

=== Underlying snapshot ===
Symbol: {symbol} ({'INDEX' if is_index else 'STOCK'})
Spot: {chain_summary.get('spot')}
ATM strike: {chain_summary.get('atm_strike')}
ATM IV: {chain_summary.get('atm_iv')}%
IV rank (in-chain proxy): {iv_rank}%
PCR (OI): {pcr}
Max pain: {max_pain}
Days to expiry: {days_to_expiry}

=== Equity bot signal ===
Signal: {equity_signal.get('signal')} | Strength: {equity_signal.get('strength')} | Confidence: {equity_signal.get('confidence')}

=== Research (news + global cues) ===
{research.get('research_summary')}

=== Available strategies ===
{strategies_list}

Think through: direction bias, volatility regime, time decay, risk/reward.
Then decide: open a trade or skip.

You MUST end your response with EXACTLY this block (no extra text after):
TRADE: YES
STRATEGY: <one of the listed strategies>
STRIKE: <integer or decimal, should be a listed chain strike near your thesis>
EXPIRY: WEEKLY
CONFIDENCE: <integer 0-100>

If skipping:
TRADE: NO
CONFIDENCE: <integer 0-100>

Leave STRATEGY/STRIKE/EXPIRY off when TRADE is NO."""

    if not DEEPSEEK_API_KEY:
        logger.warning("[FO Reasoning] DEEPSEEK_API_KEY not set")
        return None, "DEEPSEEK_API_KEY not set"

    try:
        resp = requests.post(
            f"{DEEPSEEK_BASE_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": REASONING_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 8000,
                "temperature": 0.1,
            },
            timeout=180,
        )
        if resp.status_code != 200:
            logger.warning(f"[FO Reasoning] R1 HTTP {resp.status_code}: {resp.text[:200]}")
            return None, f"HTTP {resp.status_code}"

        choice = resp.json()["choices"][0]
        message = choice.get("message", {})
        content = (message.get("content") or "").strip()
        reasoning = (message.get("reasoning_content") or "").strip()

        decision = _parse_decision(content) or _parse_decision(reasoning)
        if decision is None:
            logger.info(
                f"[FO Reasoning] {symbol}: no parseable trailer. "
                f"content_tail={content[-200:]!r}"
            )
        else:
            logger.info(
                f"[FO Reasoning] {symbol}: TRADE={decision['trade']} "
                f"STRATEGY={decision.get('strategy')} STRIKE={decision.get('strike')} "
                f"EXPIRY={decision.get('expiry')} CONF={decision.get('confidence')}"
            )
        return decision, content or reasoning

    except Exception as e:
        logger.warning(f"[FO Reasoning] error for {symbol}: {e}")
        return None, str(e)


def _parse_decision(text: str) -> Optional[dict]:
    if not text:
        return None

    # Search in the last 20 lines for the trailer block (be tolerant).
    tail = "\n".join(text.strip().split("\n")[-20:])

    trade = _match(r"TRADE:\s*(YES|NO)", tail)
    if not trade:
        return None
    trade = trade.upper()

    conf_raw = _match(r"CONFIDENCE:\s*(\d{1,3})", tail)
    try:
        confidence = int(conf_raw) if conf_raw else 0
    except ValueError:
        confidence = 0
    confidence = max(0, min(100, confidence))

    if trade == "NO":
        return {"trade": "NO", "confidence": confidence}

    strategy = _match(r"STRATEGY:\s*([A-Z_]+)", tail)
    if strategy and strategy.upper() not in VALID_STRATEGIES:
        strategy = None

    strike_raw = _match(r"STRIKE:\s*([0-9]+(?:\.[0-9]+)?)", tail)
    try:
        strike = float(strike_raw) if strike_raw else None
    except ValueError:
        strike = None

    expiry = _match(r"EXPIRY:\s*(WEEKLY|MONTHLY)", tail)
    expiry = expiry.upper() if expiry else None

    if not strategy or strike is None or not expiry:
        return None  # incomplete YES decision — reject

    return {
        "trade": "YES",
        "strategy": strategy.upper(),
        "strike": strike,
        "expiry": expiry,
        "confidence": confidence,
    }


def _match(pattern: str, text: str) -> Optional[str]:
    m = re.search(pattern, text, flags=re.IGNORECASE)
    return m.group(1) if m else None
