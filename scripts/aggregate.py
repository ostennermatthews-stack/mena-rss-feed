import hashlib
import html
import os
import re
from datetime import datetime, timezone
from email.utils import format_datetime
from typing import Any, Dict, List

import feedparser
import yaml
from dateutil import parser as dtparser

ROOT = os.path.dirname(os.path.dirname(__file__))
CFG_PATH = os.path.join(ROOT, "feeds.yaml")
OUT_PATH = os.path.join(ROOT, "feed.xml")


def load_cfg() -> Dict[str, Any]:
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip().lower()


def stable_guid(entry: Dict[str, Any]) -> str:
    raw = (
        entry.get("id")
        or entry.get("guid")
        or entry.get("link")
        or f"{entry.get('title','')}|{entry.get('published','')}"
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def parse_dt(entry: Dict[str, Any]) -> datetime:
    for k in ("published", "updated"):
        v = entry.get(k)
        if v:
            try:
                dt = dtparser.parse(v)
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                pass
    return datetime(1970, 1, 1, tzinfo=timezone.utc)


def matches_keywords(entry: Dict[str, Any], keywords: List[str]) -> bool:
    if not keywords:
        return True
    hay = " ".join(
        [
            norm(entry.get("title", "")),
            norm(entry.get("summary", "")),
            norm(entry.get("description", "")),
        ]
    )
    return any(k in hay for k in keywords)


def esc(s: str) -> str:
    return html.escape(s or "", quote=False)


def build_rss(cfg: Dict[str, Any], items: List[Dict[str, Any]]) -> str:
    title = cfg.get("title", "Aggregated Feed")
    desc = cfg.get("description", "Combined RSS feed.")
    now = datetime.now(timezone.utc)

    out = []
    out.append('<?xml version="1.0" encoding="UTF-8"?>')
    out.append('<rss version="2.0">')
    out.append("<channel>")
    out.append(f"<title>{esc(title)}</title>")
    out.append(f"<description>{esc(desc)}</description>")
    out.append(f"<lastBuildDate>{format_datetime(now)}</lastBuildDate>")

    for it in items:
        out.append("<item>")
        out.append(f"<title>{esc(it['title'])}</title>")
        out.append(f"<link>{esc(it['link'])}</link>")
        out.append(f"<guid isPermaLink=\"false\">{it['guid']}</guid>")
        out.append(f"<pubDate>{format_datetime(it['dt'])}</pubDate>")
        description = f"[{it['source']}] {it.get('summary','')}".strip()
        out.append(f"<description>{esc(description)}</description>")
        out.append("</item>")

    out.append("</channel>")
    out.append("</rss>")
    return "\n".join(out)


def main():
    cfg = load_cfg()
    feeds = cfg.get("feeds", [])
    keywords = [norm(k) for k in (cfg.get("keywords") or [])]
    max_items = int(cfg.get("max_items", 80))

    seen = set()
    items: List[Dict[str, Any]] = []

    for f in feeds:
        name = f["name"]
        url = f["url"]

        parsed = feedparser.parse(url)

        for e in parsed.entries:
            if not matches_keywords(e, keywords):
                continue

            guid = stable_guid(e)
            if guid in seen:
                continue
            seen.add(guid)

            link = e.get("link", "")
            if not link:
                continue

            dt = parse_dt(e)
            title = e.get("title", "(no title)")
            summary = e.get("summary", "") or e.get("description", "")

            items.append(
                {
                    "guid": guid,
                    "dt": dt,
                    "title": title,
                    "link": link,
                    "summary": summary,
                    "source": name,
                }
            )

    items.sort(key=lambda x: x["dt"], reverse=True)
    items = items[:max_items]

    rss = build_rss(cfg, items)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(rss)

    print(f"Wrote {OUT_PATH} with {len(items)} items")


if __name__ == "__main__":
    main()
