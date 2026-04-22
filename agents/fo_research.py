"""F&O research agent — DeepSeek V3.

Fetches:
  - GDELT/Google News for the symbol
  - Global cues (VIX, SGX, US futures, crude, gold, DXY)

Produces a 3-5 sentence factual summary for the R1 reasoning agent.
Mirrors phani-market-v2/agents/research.py structure.
"""

import os
import asyncio
import requests
from typing import Optional
from loguru import logger

from data.gdelt import get_news_context
from data.global_cues import fetch_global_cues, format_cues_for_prompt


DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1"
RESEARCH_MODEL = "deepseek-chat"  # DeepSeek V3


async def research_symbol(symbol: str, company_name: Optional[str], chain_summary: dict) -> dict:
    """Build research context for a symbol. Returns enriched dict for reasoning."""
    search_query = f"{company_name} stock" if company_name else symbol
    # Blocking calls offloaded to threads
    news_context = await asyncio.to_thread(get_news_context, search_query)
    cues = await fetch_global_cues()
    cues_str = format_cues_for_prompt(cues)

    summary_text = await asyncio.to_thread(
        _summarize, symbol, news_context, cues_str, chain_summary
    )

    return {
        "symbol": symbol,
        "news_context": news_context,
        "global_cues": cues,
        "global_cues_str": cues_str,
        "research_summary": summary_text,
    }


def _summarize(symbol: str, news: str, cues_str: str, chain_summary: dict) -> str:
    prompt = f"""You are a research analyst for an Indian F&O (Nifty/BankNifty/stock options) trading bot.

Symbol: {symbol}
Spot: {chain_summary.get('spot')} | ATM strike: {chain_summary.get('atm_strike')} | ATM IV: {chain_summary.get('atm_iv')}%

Recent News:
{news}

Global Market Cues:
{cues_str}

Write a factual 3-5 sentence research summary covering:
1. Key news/events affecting this symbol today
2. Overall risk-on or risk-off tilt from global cues
3. Anything noteworthy about the options chain (IV level, spot vs ATM)

Be concise and objective. Do NOT recommend a trade or predict direction — just the facts."""

    if not DEEPSEEK_API_KEY:
        logger.warning("[FO Research] DEEPSEEK_API_KEY not set — returning raw news only")
        return f"News: {news[:400]}\n\nGlobal cues:\n{cues_str}"

    try:
        resp = requests.post(
            f"{DEEPSEEK_BASE_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": RESEARCH_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 350,
                "temperature": 0.3,
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logger.warning(f"[FO Research] V3 summarise failed for {symbol}: {e}")
        return f"News: {news[:300]}\n\nCues:\n{cues_str}"
