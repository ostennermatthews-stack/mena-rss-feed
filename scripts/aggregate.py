import hashlib
import html
import os
import re
import unicodedata
from datetime import datetime, timezone
from email.utils import format_datetime
from typing import Any, Dict, List
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

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


def esc(s: str) -> str:
    return html.escape(s or "", quote=False)


def normalize_title(title: str) -> str:
    """
    Normalize titles for consistency + better Slack readability.
    """
    t = (title or "").strip()
    t = unicodedata.normalize("NFKC", t)
    t = re.sub(r"\s+", " ", t)

    # Remove common trailing wire/source tags
    t = re.sub(
        r"\s+[-–—|:]\s*(Reuters|AP|AFP)\s*$",
        "",
        t,
        flags=re.I,
    )

    # Remove generic trailing site names like " | Some Site" or " - Some Site"
    # Keep it conservative to avoid over-stripping meaningful titles.
    t = re.sub(r"\s+[-–—|]\s*[^-–—|]{2,40}\s*$", "", t)

    t = t.strip("“”\"' ")
    return t or "(no title)"


def canonicalize_url(url: str) -> str:
    """
    Reduce duplicates from tracking params (utm_*, fbclid, etc.)
    Drops fragment always; keeps only a small allowlist of query params.
    """
    if not url:
        return ""
    try:
        p = urlparse(url)
        p = p._replace(fragment="")

        allow = {"id", "story", "article", "p"}
        qs = parse_qsl(p.query, keep_blank_values=True)
        qs2 = [(k, v) for (k, v) in qs if k.lower() in allow]
        new_query = urlencode(qs2, doseq=True)

        p = p._replace(query=new_query)
        return urlunparse(p)
    except Exception:
        return url


def extract_plain_text(html_text: str) -> str:
    """
    Strip HTML so Slack doesn't show messy markup.
    """
    if not html_text:
        return ""
    s = unicodedata.normalize("NFKC", html_text)

    # Remove script/style blocks
    s = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", s)

    # Remove all tags
    s = re.sub(r"(?s)<[^>]+>", " ", s)

    # Decode entities, then normalize whitespace
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def smart_truncate(text: str, limit: int) -> str:
    if not text:
        return ""
    if len(text) <= limit:
        return text
    cut = text[:limit]
    if " " in cut:
        cut = cut.rsplit(" ", 1)[0]
    return cut + "…"


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
            norm(extract_plain_text(entry.get("summary", ""))),
            norm(extract_plain_text(entry.get("description", ""))),
        ]
    )
    return any(k in hay for k in keywords)


def stable_guid(entry: Dict[str, Any], link: str) -> str:
    """
    Stable item GUID. Prefer entry ids, else link.
    """
    raw = entry.get("id") or entry.get("guid") or link or (
        f"{entry.get('title','')}|{entry.get('published','')}"
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def story_fingerprint(title: str, link: str) -> str:
    """
    Aggressive cross-source dedupe:
    - normalized title + host+path
    """
    t = normalize_title(title).lower()
    u = canonicalize_url(link)
    try:
        p = urlparse(u)
        hostpath = f"{p.netloc.lower()}{p.path}"
    except Exception:
        hostpath = u.lower()
    raw = f"{t}|{hostpath}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_rss(cfg: Dict[str, Any], items: List[Dict[str, Any]]) -> str:
    title = cfg.get("title", "Aggregated Feed")
    desc = cfg.get("description", "Combined RSS feed.")
    now = datetime.now(timezone.utc)

    # Slack readability knobs
    SUMMARY_LIMIT = 360
    SEPARATOR = "—" * 16

    out = []
    out.append('<?xml version="1.0" encoding="UTF-8"?>')
    out.append('<rss version="2.0">')
    out.append("<channel>")
    out.append(f"<title>{esc(title)}</title>")
    out.append(f"<description>{esc(desc)}</description>")
    out.append(f"<lastBuildDate>{format_datetime(now)}</lastBuildDate>")

    for it in items:
        summary = smart_truncate((it.get("summary") or "").strip(), SUMMARY_LIMIT)
        ts = it["dt"].astimezone(timezone.utc).strftime("%b %d, %Y %H:%M UTC")

        # Slack-friendly: structured plain text
        formatted = (
            f"{it['source']} • {ts}\n"
            f"{SEPARATOR}\n"
            f"{summary}\n"
            f"{SEPARATOR}\n"
            f"Read more: {it['link']}"
        ).strip()

        # Prefix the title with source for easy scanning in Slack
        slack_title = f"{it['source']} | {it['title']}"

        out.append("<item>")
        out.append(f"<title>{esc(slack_title)}</title>")
        out.append(f"<link>{esc(it['link'])}</link>")
        out.append(f"<guid isPermaLink=\"false\">{it['guid']}</guid>")
        out.append(f"<pubDate>{format_datetime(it['dt'])}</pubDate>")
        out.append(f"<description>{esc(formatted)}</description>")
        out.append("</item>")

    out.append("</channel>")
    out.append("</rss>")
    return "\n".join(out)


def main():
    cfg = load_cfg()
    feeds = cfg.get("feeds", [])
    keywords = [norm(k) for k in (cfg.get("keywords") or [])]
    max_items = int(cfg.get("max_items", 80))

    # Use one set for both guid + fingerprint
    seen = set()
    items: List[Dict[str, Any]] = []

    for f in feeds:
        name = f["name"]
        url = f["url"]

        parsed = feedparser.parse(url)

        for e in parsed.entries:
            if not matches_keywords(e, keywords):
                continue

            link = canonicalize_url(e.get("link", "") or "")
            if not link:
                continue

            dt = parse_dt(e)
            title = normalize_title(e.get("title", "(no title)"))

            raw_summary = e.get("summary", "") or e.get("description", "")
            summary = extract_plain_text(raw_summary)

            guid = stable_guid(e, link)
            fp = story_fingerprint(title, link)

            # Aggressive dedupe: same guid OR same (normalized title + host/path)
            if guid in seen or fp in seen:
                continue
            seen.add(guid)
            seen.add(fp)

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
