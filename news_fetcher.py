"""
news_fetcher.py
Real-time news intelligence for NSE stocks.

Sources:
- MoneyControl RSS feeds (stock-specific news)
- Economic Times Markets RSS
- NSE announcements
- Google News (per stock)
- RBI/SEBI announcements
- Economic calendar (RBI policy, inflation, GDP)
"""

import httpx
import asyncio
import feedparser
from datetime import datetime, timezone, timedelta
from typing import List, Optional
from dataclasses import dataclass
from loguru import logger


@dataclass
class NewsItem:
    title: str
    summary: str
    source: str
    url: str
    published_at: Optional[datetime] = None
    relevance_score: float = 0.5
    sentiment: str = "neutral"  # bullish / bearish / neutral


# ── RSS Feed Sources ──────────────────────────────────────────────────────────
MARKET_RSS_FEEDS = [
    {
        "name": "Economic Times Markets",
        "url": "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
        "weight": 1.0,
    },
    {
        "name": "MoneyControl Markets",
        "url": "https://www.moneycontrol.com/rss/MCtopnews.xml",
        "weight": 0.9,
    },
    {
        "name": "Business Standard Markets",
        "url": "https://www.business-standard.com/rss/markets-106.rss",
        "weight": 0.9,
    },
    {
        "name": "Mint Markets",
        "url": "https://www.livemint.com/rss/markets",
        "weight": 0.8,
    },
    {
        "name": "NSE India News",
        "url": "https://www.nseindia.com/api/press-release?index=equities",
        "weight": 1.0,
    },
]

# Economic events that affect all stocks
MACRO_KEYWORDS = [
    "RBI", "repo rate", "inflation", "CPI", "GDP", "fiscal deficit",
    "FII", "FDI", "rupee", "dollar", "crude oil", "budget",
    "SEBI", "GST", "interest rate", "monetary policy", "FOMC",
    "earnings", "results", "quarterly", "Q1", "Q2", "Q3", "Q4",
]

# Bullish keywords
BULLISH_KEYWORDS = [
    "beat", "surge", "rally", "gain", "growth", "profit", "record",
    "upgrade", "buy", "strong", "positive", "rise", "jump", "outperform",
    "expansion", "deal", "order", "contract", "acquisition", "dividend",
]

# Bearish keywords
BEARISH_KEYWORDS = [
    "miss", "fall", "decline", "loss", "drop", "weak", "negative",
    "downgrade", "sell", "concern", "risk", "penalty", "fine", "probe",
    "slowdown", "cut", "layoff", "debt", "default", "investigation",
]


class NewsIntelligence:
    """
    Fetches and analyses news for NSE stocks.
    Provides sentiment-tagged, relevance-scored news items
    to feed into Claude's reasoning engine.
    """

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0 (compatible; StockBot/1.0)"},
            follow_redirects=True,
        )
        self._cache: dict = {}
        self._cache_time: dict = {}
        self.CACHE_MINUTES = 15

    async def close(self):
        await self.client.aclose()

    def _is_cached(self, key: str) -> bool:
        if key not in self._cache_time:
            return False
        age = (datetime.now() - self._cache_time[key]).seconds / 60
        return age < self.CACHE_MINUTES

    def _score_relevance(self, text: str, symbol: str, company_name: str) -> float:
        """Score how relevant a news item is to a specific stock."""
        text_lower = text.lower()
        symbol_lower = symbol.lower()
        company_lower = company_name.lower().split()[0]  # First word of company name

        score = 0.0

        # Direct mention = high relevance
        if symbol_lower in text_lower:
            score += 0.6
        if company_lower in text_lower:
            score += 0.4

        # Sector/market keywords = moderate relevance
        for kw in MACRO_KEYWORDS:
            if kw.lower() in text_lower:
                score += 0.1
                break

        return min(score, 1.0)

    def _detect_sentiment(self, text: str) -> str:
        """Detect bullish/bearish/neutral sentiment from text."""
        text_lower = text.lower()
        bullish_count = sum(1 for kw in BULLISH_KEYWORDS if kw in text_lower)
        bearish_count = sum(1 for kw in BEARISH_KEYWORDS if kw in text_lower)

        if bullish_count > bearish_count:
            return "bullish"
        elif bearish_count > bullish_count:
            return "bearish"
        return "neutral"

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse various date formats from RSS feeds."""
        if not date_str:
            return None
        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S GMT",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).replace(tzinfo=None)
            except ValueError:
                continue
        return None

    async def fetch_rss_feed(self, feed_info: dict) -> List[NewsItem]:
        """Fetch and parse a single RSS feed."""
        try:
            response = await self.client.get(feed_info["url"])
            if response.status_code != 200:
                return []

            feed = feedparser.parse(response.text)
            items = []

            for entry in feed.entries[:20]:  # Last 20 items per feed
                title = entry.get("title", "")
                summary = entry.get("summary", entry.get("description", ""))[:300]
                url = entry.get("link", "")
                date_str = entry.get("published", entry.get("updated", ""))
                published = self._parse_date(date_str)

                # Only include news from last 48 hours
                if published:
                    age_hours = (datetime.now() - published).total_seconds() / 3600
                    if age_hours > 48:
                        continue

                items.append(NewsItem(
                    title=title,
                    summary=summary,
                    source=feed_info["name"],
                    url=url,
                    published_at=published,
                    sentiment=self._detect_sentiment(title + " " + summary),
                ))
            return items

        except Exception as e:
            logger.debug(f"RSS feed failed ({feed_info['name']}): {e}")
            return []

    async def fetch_google_news(self, symbol: str, company_name: str) -> List[NewsItem]:
        """Fetch Google News RSS for a specific stock."""
        query = f"{company_name} NSE stock"
        url = f"https://news.google.com/rss/search?q={query}&hl=en-IN&gl=IN&ceid=IN:en"
        try:
            response = await self.client.get(url)
            if response.status_code != 200:
                return []

            feed = feedparser.parse(response.text)
            items = []

            for entry in feed.entries[:10]:
                title = entry.get("title", "")
                url = entry.get("link", "")
                date_str = entry.get("published", "")
                published = self._parse_date(date_str)

                if published:
                    age_hours = (datetime.now() - published).total_seconds() / 3600
                    if age_hours > 48:
                        continue

                # Only include if relevant to this stock
                relevance = self._score_relevance(title, symbol, company_name)
                if relevance > 0.3:
                    items.append(NewsItem(
                        title=title,
                        summary="",
                        source="Google News",
                        url=url,
                        published_at=published,
                        relevance_score=relevance,
                        sentiment=self._detect_sentiment(title),
                    ))
            return items

        except Exception as e:
            logger.debug(f"Google News failed for {symbol}: {e}")
            return []

    async def get_market_news(self) -> List[NewsItem]:
        """Fetch broad market news from all RSS sources."""
        cache_key = "market_news"
        if self._is_cached(cache_key):
            return self._cache[cache_key]

        tasks = [self.fetch_rss_feed(feed) for feed in MARKET_RSS_FEEDS[:4]]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_items = []
        for result in results:
            if isinstance(result, list):
                all_items.extend(result)

        # Sort by recency
        all_items.sort(
            key=lambda x: x.published_at or datetime.min,
            reverse=True
        )

        self._cache[cache_key] = all_items[:30]
        self._cache_time[cache_key] = datetime.now()
        return self._cache[cache_key]

    async def get_stock_news(
        self, symbol: str, company_name: str
    ) -> List[NewsItem]:
        """Get news specifically relevant to a stock."""
        cache_key = f"stock_{symbol}"
        if self._is_cached(cache_key):
            return self._cache[cache_key]

        # Fetch market news + Google News in parallel
        market_news_task = self.get_market_news()
        google_news_task = self.fetch_google_news(symbol, company_name)

        market_news, google_news = await asyncio.gather(
            market_news_task, google_news_task, return_exceptions=True
        )

        combined = []

        # Score market news for this stock
        if isinstance(market_news, list):
            for item in market_news:
                relevance = self._score_relevance(
                    item.title + " " + item.summary, symbol, company_name
                )
                if relevance > 0.1:
                    item.relevance_score = relevance
                    combined.append(item)

        # Add Google News (already filtered)
        if isinstance(google_news, list):
            combined.extend(google_news)

        # Sort by relevance then recency
        combined.sort(
            key=lambda x: (x.relevance_score, x.published_at or datetime.min),
            reverse=True
        )

        # Deduplicate by title similarity
        seen_titles = set()
        unique = []
        for item in combined:
            title_key = item.title[:50].lower()
            if title_key not in seen_titles:
                seen_titles.add(title_key)
                unique.append(item)

        top_news = unique[:8]  # Top 8 most relevant items
        self._cache[cache_key] = top_news
        self._cache_time[cache_key] = datetime.now()
        return top_news

    def format_for_prompt(self, news_items: List[NewsItem]) -> str:
        """Format news items for Claude's reasoning prompt."""
        if not news_items:
            return "No recent news found. Base analysis purely on technicals and fundamentals."

        lines = []
        for i, item in enumerate(news_items[:6]):
            age_str = ""
            if item.published_at:
                hours_ago = (datetime.now() - item.published_at).total_seconds() / 3600
                if hours_ago < 24:
                    age_str = f" [{int(hours_ago)}h ago]"
                else:
                    age_str = f" [{int(hours_ago/24)}d ago]"

            sentiment_icon = {
                "bullish": "🟢",
                "bearish": "🔴",
                "neutral": "⚪"
            }.get(item.sentiment, "⚪")

            lines.append(
                f"[{i+1}] {sentiment_icon} {item.source}{age_str} | "
                f"Relevance: {item.relevance_score:.0%}\n"
                f"    {item.title}\n"
                f"    {item.summary[:150] if item.summary else ''}"
            )

        return "\n\n".join(lines)

    def get_news_summary_stats(self, news_items: List[NewsItem]) -> dict:
        """Get summary statistics from news items."""
        if not news_items:
            return {"bullish": 0, "bearish": 0, "neutral": 0, "total": 0, "bias": "neutral"}

        counts = {"bullish": 0, "bearish": 0, "neutral": 0}
        for item in news_items:
            counts[item.sentiment] = counts.get(item.sentiment, 0) + 1

        total = len(news_items)
        if counts["bullish"] > counts["bearish"] * 1.5:
            bias = "bullish"
        elif counts["bearish"] > counts["bullish"] * 1.5:
            bias = "bearish"
        else:
            bias = "neutral"

        return {**counts, "total": total, "bias": bias}
