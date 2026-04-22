"""News fetcher. Historically GDELT — now Google News RSS.
Module name kept for import stability across the codebase.
Copied verbatim from phani-market-v2/data/gdelt.py.
"""
import time
import requests
import xml.etree.ElementTree as ET
from urllib.parse import quote_plus

_GOOGLE_NEWS_RSS = "https://news.google.com/rss/search"
_HEADERS = {"User-Agent": "Mozilla/5.0 (nse-stock-bot/1.0)"}
_last_call_ts = 0.0
_MIN_GAP_SECONDS = 1.0


def get_news_context(query: str, max_articles: int = 5) -> str:
    """Fetch recent news headlines for a search query via Google News RSS.
    For F&O the caller passes the company name / symbol instead of a market question."""
    global _last_call_ts
    try:
        gap = time.time() - _last_call_ts
        if gap < _MIN_GAP_SECONDS:
            time.sleep(_MIN_GAP_SECONDS - gap)

        cleaned = _extract_search_query(query)
        if not cleaned:
            return "No recent news found."

        url = f"{_GOOGLE_NEWS_RSS}?q={quote_plus(cleaned)}&hl=en-IN&gl=IN&ceid=IN:en"
        resp = requests.get(url, headers=_HEADERS, timeout=15)
        _last_call_ts = time.time()
        if resp.status_code == 429:
            return "News fetch rate-limited."
        resp.raise_for_status()

        items = _parse_rss(resp.content, max_articles)
        if not items:
            return "No recent news found."
        return "\n".join(f"- [{it['date']}] {it['title']} ({it['source']})" for it in items)

    except Exception as e:
        print(f"[News] Error fetching: {e}", flush=True)
        return "News fetch failed."


def _parse_rss(content: bytes, limit: int) -> list:
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return []
    out = []
    for item in root.findall(".//item")[:limit]:
        title_el = item.find("title")
        source_el = item.find("source")
        date_el = item.find("pubDate")
        title = (title_el.text or "").strip() if title_el is not None else ""
        if not title:
            continue
        source = (source_el.text or "").strip() if source_el is not None else ""
        date = ""
        if date_el is not None and date_el.text:
            parts = date_el.text.split(" ")
            if len(parts) >= 4:
                date = " ".join(parts[1:4])
        out.append({"title": title, "source": source, "date": date})
    return out


def _extract_search_query(text: str) -> str:
    stop_words = {"will", "the", "a", "an", "be", "in", "by", "to", "of",
                  "and", "or", "is", "are", "was", "were", "has", "have",
                  "that", "this", "for", "with", "at", "from", "before",
                  "after", "during", "between", "who", "what", "when",
                  "where", "which", "how", "do", "does", "did"}
    words = text.lower().replace("?", "").replace(",", "").split()
    keywords = [w for w in words if w not in stop_words and len(w) > 2]
    return " ".join(keywords[:5]) if keywords else text.strip()
