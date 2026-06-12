#!/usr/bin/env python3
"""diag_parfumo_resolve.py -- Parfumo fallback resolver PROOF OF CONCEPT.

Read-only diagnostic. Does NOT write to Postgres, does NOT touch enrichment
jobs, and does NOT modify the engine. It demonstrates the three pieces a
production Parfumo fallback would need, so the design can be reviewed before any
wiring lands:

  1. URL discovery   -- structured search provider scoped to site:parfumo.com,
                        with a capped single-house brand-page crawl fallback.
  2. Public parsing  -- factual product fields only (no review text) from the
                        public perfume page.
  3. Identity match  -- reuses engine.Orchestrator.identity_score / IdentityTools
                        so Parfumo accept/reject mirrors the Fragrantica matcher.

Compliance: only public /Perfumes and /Parfums pages and public brand pages are
fetched. No /api/, /app_v5/api/, /s_perfumes.php, or logged-in endpoints. Every
network call is rate-limited (--delay) and Parfumo is treated as a fallback, not
a bulk crawl.

Usage:
    # Built-in regression cases (uses seeded URLs when the provider is disabled):
    python scripts/diag_parfumo_resolve.py --cases

    # One ad-hoc fragrance:
    python scripts/diag_parfumo_resolve.py --house "Lattafa" --name "Ramz Gold"

    # Allow the capped brand-page crawl fallback (single house, first N pages):
    python scripts/diag_parfumo_resolve.py --house "Clive Christian" \
        --name "Miami Poolside" --brand-crawl --max-pages 6

    # Score a specific candidate URL directly (skips discovery):
    python scripts/diag_parfumo_resolve.py --house "Lattafa" --name "Ramz Gold" \
        --url https://www.parfumo.com/Perfumes/Lattafa/ramz-lattafa-gold
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import fragrance_parser_full_rewrite_fixed as engine  # noqa: E402
from bs4 import BeautifulSoup  # noqa: E402

# --------------------------------------------------------------------------
# Thresholds (proposed; mirror Orchestrator.CATALOG_ACCEPT but a touch stricter
# because Parfumo is a fallback and we prefer precision over recall here).
# --------------------------------------------------------------------------
ACCEPT = 0.82          # >= ACCEPT and clear of the runner-up -> auto-accept
MANUAL_REVIEW = 0.70   # [MANUAL_REVIEW, ACCEPT) -> queue for a human
SIBLING_MARGIN = 0.06  # top two within this band -> manual review (collision)

PARFUMO_HOST = "www.parfumo.com"
# Public perfume page path: /Perfumes/<Brand>/<slug> or the /Parfums alias.
_PERFUME_PATH_RE = re.compile(r"^/(?:Perfumes|Parfums)/[^/]+/[^/?#]+$")
_BRAND_PATH_RE = re.compile(r"^/(?:Perfumes|Parfums)/[^/?#]+/?$")


# ==========================================================================
# 1. URL canonicalization / shape
# ==========================================================================
def canonical_parfumo_url(url: str) -> str:
    """Normalize a Parfumo URL: https + www host, drop query/fragment, no
    trailing slash. The /Parfums alias is NOT rewritten here -- the live page's
    og:url already points at the canonical /Perfumes form, so canonicalization
    of confirmed hits happens after the fetch (see parse_fields)."""
    text = (url or "").strip()
    if not text:
        return ""
    if "://" not in text and text.lower().startswith(("parfumo.com", "www.parfumo.com")):
        text = "https://" + text
    parsed = urlparse(text)
    host = (parsed.netloc or "").lower()
    if host.endswith("parfumo.com"):
        host = PARFUMO_HOST
    path = (parsed.path or "").rstrip("/")
    return f"https://{host}{path}" if host else ""


def is_perfume_url(url: str) -> bool:
    parsed = urlparse(canonical_parfumo_url(url))
    return parsed.netloc == PARFUMO_HOST and bool(_PERFUME_PATH_RE.match(parsed.path or ""))


def brand_slug(house: str) -> str:
    """Parfumo brand slugs keep case and use underscores for spaces."""
    cleaned = engine.TextSanitizer.clean(house)
    return re.sub(r"\s+", "_", cleaned).strip("_")


# ==========================================================================
# 2. Parsing -- factual fields only, no review text
# ==========================================================================
@dataclass
class ParfumoRecord:
    url: str
    name: str = ""
    house: str = ""
    year: str = ""
    gender: str = ""
    rating: str = ""
    rating_count: str = ""
    accords: list[str] = field(default_factory=list)
    notes_top: list[str] = field(default_factory=list)
    notes_heart: list[str] = field(default_factory=list)
    notes_base: list[str] = field(default_factory=list)
    notes_flat: list[str] = field(default_factory=list)
    perfumers: list[str] = field(default_factory=list)
    status: str = ""
    source: str = "parfumo"


def _gender_from_desc(desc: str) -> str:
    low = desc.lower()
    has_w = "for women" in low or "women and men" in low or "men and women" in low
    has_m = "for men" in low or "women and men" in low or "men and women" in low
    if has_w and has_m:
        return "Unisex"
    if "for women" in low:
        return "Feminine"
    if "for men" in low:
        return "Masculine"
    return ""


def _status_from_desc(desc: str) -> str:
    low = desc.lower()
    if "still in production" in low:
        return "in_production"
    if "discontinued" in low:
        return "discontinued"
    return ""


def _parse_notes(soup: BeautifulSoup) -> dict[str, list[str]]:
    """Parfumo renders the pyramid as a .notes_list with 'Top/Heart/Base Notes'
    section headers followed by individual .clickable_note_img spans. Flanker /
    travel sizes (e.g. Miami Poolside) have a flat list with no section headers."""
    out = {"top": [], "heart": [], "base": [], "flat": []}
    container = soup.select_one(".notes_list")
    if not container:
        return out
    spans = container.select(".clickable_note_img")
    note_texts = [engine.TextSanitizer.clean(s.get_text(" ", strip=True)) for s in spans]
    note_texts = [n for n in note_texts if n]
    full = container.get_text(" ", strip=True).lower()
    if "top notes" in full or "base notes" in full or "heart notes" in full:
        # Walk the container text to bucket notes under their section header.
        current = "flat"
        header_map = {"top notes": "top", "heart notes": "heart",
                      "middle notes": "heart", "base notes": "base"}
        # Re-walk using the raw section text order.
        text_blob = container.get_text("\n", strip=True)
        for line in text_blob.split("\n"):
            key = line.strip().lower()
            if key in header_map:
                current = header_map[key]
                continue
            cleaned = engine.TextSanitizer.clean(line)
            if cleaned and cleaned in note_texts:
                out[current].append(cleaned)
        # Anything the walk missed falls back to flat to avoid data loss.
        bucketed = set(out["top"]) | set(out["heart"]) | set(out["base"])
        for n in note_texts:
            if n not in bucketed and n not in out["flat"]:
                out["flat"].append(n)
        if not (out["top"] or out["heart"] or out["base"]):
            out["flat"] = note_texts
    else:
        out["flat"] = note_texts
    return out


def parse_fields(soup: BeautifulSoup, fetched_url: str) -> ParfumoRecord | None:
    """Return a ParfumoRecord, or None for a soft-404 (Parfumo serves missing
    slugs as HTTP 200 with no og:url and no breadcrumb name)."""
    og_url_node = soup.select_one('meta[property="og:url"]')
    og_url = og_url_node.get("content") if og_url_node else ""
    if not og_url or not is_perfume_url(og_url):
        return None  # soft-404 / non-perfume page

    rec = ParfumoRecord(url=canonical_parfumo_url(og_url))

    # House + name: breadcrumb is the most reliable; og:title backs it up.
    house = name = ""
    import json
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(s.string or s.get_text() or "{}")
        except Exception:
            continue
        if isinstance(data, dict) and data.get("@type") == "BreadcrumbList":
            items = data.get("itemListElement") or []
            if len(items) >= 3:
                house = str(items[2].get("name") or "")
            if items:
                name = str(items[-1].get("name") or "")
    og_title = soup.select_one('meta[property="og:title"]')
    if og_title and not name:
        title = og_title.get("content") or ""
        name = re.split(r"\s+by\s+", title)[0].strip()
    rec.house = engine.TextSanitizer.clean(house)
    rec.name = engine.TextSanitizer.clean(name)

    og_desc_node = soup.select_one('meta[property="og:description"]')
    og_desc = (og_desc_node.get("content") if og_desc_node else "") or ""
    rec.gender = _gender_from_desc(og_desc)
    rec.status = _status_from_desc(og_desc)
    m = re.search(r"released in (\d{4})", og_desc)
    if m:
        rec.year = m.group(1)
    else:
        m = re.search(r"\b(?:19|20)\d{2}\b", og_desc)
        rec.year = m.group(0) if m else ""
    m2 = re.search(r"scent is ([a-z\-]+)", og_desc.lower())
    if m2:
        rec.accords = [m2.group(1)]

    rv = soup.find(attrs={"itemprop": "ratingValue"})
    rc = soup.find(attrs={"itemprop": "ratingCount"})
    if rv:
        rec.rating = (rv.get("content") or rv.get_text(strip=True) or "").strip()
    if rc:
        rec.rating_count = (rc.get("content") or rc.get_text(strip=True) or "").strip()

    notes = _parse_notes(soup)
    rec.notes_top, rec.notes_heart = notes["top"], notes["heart"]
    rec.notes_base, rec.notes_flat = notes["base"], notes["flat"]

    perf: list[str] = []
    for a in soup.find_all("a", href=True):
        if "/Perfumers/" in a.get("href", ""):
            t = engine.TextSanitizer.clean(a.get_text(strip=True))
            if t and t not in perf:
                perf.append(t)
    rec.perfumers = perf[:5]
    return rec


def fetch_record(scraper, url: str, *, timeout: float = 15.0) -> ParfumoRecord | None:
    res = engine.Http.get(scraper, url, timeout=timeout, attempts=2,
                          referer="https://www.parfumo.com/")
    if not res or getattr(res, "status_code", None) != 200:
        return None
    soup = BeautifulSoup(res.text or "", "html.parser")
    return parse_fields(soup, url)


# ==========================================================================
# 1b. Discovery
# ==========================================================================
def discover_via_serp(house: str, name: str, *, timeout: float = 4.0) -> list[str]:
    """Primary discovery through the engine's configured structured provider."""
    provider = engine.structured_search_provider()
    if provider is None:
        return []
    try:
        return provider.search_parfumo_urls(house, name, timeout=timeout)
    except Exception:
        return []


def discover_via_brand_crawl(
    scraper, house: str, *, max_pages: int, delay: float, page_timeout: float = 15.0
) -> list[str]:
    """Capped, single-house public brand-page crawl. Last-resort fallback only.

    Walks /Perfumes/<Brand>?current_page=N for at most max_pages, collecting the
    public perfume links. Rate-limited by --delay between page fetches. Anchor
    text on the brand grid is empty (image tiles), so the returned URLs must
    still be fetched+scored individually by the caller."""
    slug = brand_slug(house)
    if not slug:
        return []
    base = f"https://{PARFUMO_HOST}/Perfumes/{slug}"
    found: list[str] = []
    seen: set[str] = set()
    for page in range(1, max(1, max_pages) + 1):
        url = base if page == 1 else f"{base}?current_page={page}"
        res = engine.Http.get(scraper, url, timeout=page_timeout, attempts=2,
                              referer="https://www.parfumo.com/")
        if not res or getattr(res, "status_code", None) != 200:
            break
        soup = BeautifulSoup(res.text or "", "html.parser")
        page_hits = 0
        for a in soup.find_all("a", href=True):
            path = urlparse(a.get("href", "")).path
            if _PERFUME_PATH_RE.match(path):
                canonical = canonical_parfumo_url(urljoin(base, path))
                if canonical not in seen:
                    seen.add(canonical)
                    found.append(canonical)
                    page_hits += 1
        if page_hits == 0:
            break
        if delay > 0 and page < max_pages:
            time.sleep(delay)
    return found


# ==========================================================================
# 3. Identity match -- reuse the engine's scorer
# ==========================================================================
def _normalize_parfumo_name(name: str) -> str:
    """Parfumo embeds flanker markers in parens, e.g. 'Ramz Lattafa (Gold)'.
    Flatten to 'Ramz Lattafa Gold' so the engine's token logic sees the words."""
    return engine.TextSanitizer.clean(re.sub(r"[()]", " ", name or ""))


def score_record(probe_house: str, probe_name: str, probe_year: str, rec: ParfumoRecord) -> float:
    probe = engine.UnifiedFragrance(name=probe_name, brand=probe_house, year=probe_year or "")
    catalog = engine.CatalogItem(
        name=_normalize_parfumo_name(rec.name),
        brand=rec.house,
        year=rec.year or "",
        url=rec.url,
    )
    return engine.Orchestrator.identity_score(probe, catalog)


@dataclass
class Verdict:
    decision: str            # accept | manual_review | reject | not_found
    record: ParfumoRecord | None
    score: float
    runner_up: float
    discovery: str
    candidates_scored: int


def resolve(
    scraper,
    house: str,
    name: str,
    year: str = "",
    *,
    seeded_urls: list[str] | None = None,
    brand_crawl: bool = False,
    max_pages: int = 6,
    delay: float = 2.0,
) -> Verdict:
    # ---- discovery (primary -> seeded -> brand crawl) --------------------
    provider = engine.structured_search_provider()
    discovery = getattr(provider, "PROVIDER_NAME", "structured") if provider else "none"
    urls = discover_via_serp(house, name) if provider else []
    if not urls and seeded_urls:
        discovery = "seeded"
        urls = [canonical_parfumo_url(u) for u in seeded_urls if is_perfume_url(u)]
    if not urls and brand_crawl:
        discovery = "brand_crawl"
        urls = discover_via_brand_crawl(scraper, house, max_pages=max_pages, delay=delay)
    if not urls:
        return Verdict("not_found", None, 0.0, 0.0, discovery, 0)

    # ---- fetch + parse + score every candidate ---------------------------
    scored: list[tuple[float, ParfumoRecord]] = []
    for i, url in enumerate(urls):
        rec = fetch_record(scraper, url)
        if rec is None:
            continue
        scored.append((score_record(house, name, year, rec), rec))
        if delay > 0 and i < len(urls) - 1:
            time.sleep(delay)
    if not scored:
        return Verdict("not_found", None, 0.0, 0.0, discovery, 0)

    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_rec = scored[0]
    runner_up = scored[1][0] if len(scored) > 1 else 0.0

    if best_score >= ACCEPT and (best_score - runner_up) >= SIBLING_MARGIN:
        decision = "accept"
    elif best_score >= MANUAL_REVIEW:
        decision = "manual_review"  # includes sibling-collision near-ties
    else:
        decision = "reject"
    return Verdict(decision, best_rec, best_score, runner_up, discovery, len(scored))


# ==========================================================================
# CLI
# ==========================================================================
# Seeded URLs let the parser+matcher path be demonstrated end-to-end even when
# In local environments the structured provider can be disabled. In production,
# discovery uses the configured provider first, then the capped brand crawl;
# these literals would not exist.
_CASES = [
    # (house, name, year, expect, seeded_urls)
    ("Lattafa", "Ramz Gold", "", "accept",
     ["https://www.parfumo.com/Perfumes/Lattafa/ramz-lattafa-gold"]),
    ("Calvin Klein", "Man", "", "accept",
     ["https://www.parfumo.com/Perfumes/Calvin_Klein/Calvin_Klein_Man_Eau_de_Toilette"]),
    ("Clive Christian", "Miami Poolside", "2019", "accept",
     ["https://www.parfumo.com/Perfumes/Clive_Christian/the-art-of-travel-collection-miami-poolside"]),
    # Near-miss regressions: right house, wrong perfume -> must NOT auto-accept.
    ("Calvin Klein", "Man", "", "reject",
     ["https://www.parfumo.com/Perfumes/Calvin_Klein/Eternity_for_Men_Eau_de_Toilette"]),
    ("Calvin Klein", "Man", "", "reject",
     ["https://www.parfumo.com/Perfumes/Calvin_Klein/CK_One"]),
    # Unrelated house -> must reject on the brand gate.
    ("Lattafa", "Ramz Gold", "", "reject",
     ["https://www.parfumo.com/Perfumes/Calvin_Klein/CK_Be"]),
]


def _print_record(rec: ParfumoRecord) -> None:
    print(f"      name={rec.name!r} house={rec.house!r} year={rec.year or '—'} "
          f"gender={rec.gender or '—'} rating={rec.rating or '—'}/{rec.rating_count or '—'}")
    print(f"      status={rec.status or '—'} accords={rec.accords} perfumers={rec.perfumers}")
    print(f"      notes top={rec.notes_top} heart={rec.notes_heart} "
          f"base={rec.notes_base} flat={rec.notes_flat}")


def run_cases(scraper, delay: float) -> int:
    print("Parfumo fallback resolver POC -- regression cases\n")
    failures = 0
    for house, name, year, expect, seeded in _CASES:
        v = resolve(scraper, house, name, year, seeded_urls=seeded, delay=delay)
        ok = (v.decision == expect) or (expect == "accept" and v.decision == "accept")
        # For 'reject' expectations, manual_review is also acceptable (not auto-accepted).
        if expect == "reject" and v.decision in ("reject", "manual_review", "not_found"):
            ok = True
        flag = "PASS" if ok else "FAIL"
        if not ok:
            failures += 1
        print(f"[{flag}] {house} / {name!r} (expect {expect}) -> {v.decision} "
              f"score={v.score:.3f} runner_up={v.runner_up:.3f} "
              f"via={v.discovery} scored={v.candidates_scored}")
        if v.record:
            _print_record(v.record)
        print()
    print(f"{'ALL PASS' if failures == 0 else str(failures) + ' FAILED'}")
    return 1 if failures else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cases", action="store_true", help="Run the built-in regression cases")
    ap.add_argument("--house", default="")
    ap.add_argument("--name", default="")
    ap.add_argument("--year", default="")
    ap.add_argument("--url", action="append", default=[], help="Seed candidate URL(s); skips provider discovery")
    ap.add_argument("--brand-crawl", action="store_true", help="Allow capped single-house brand crawl")
    ap.add_argument("--max-pages", type=int, default=6, help="Brand-crawl page cap")
    ap.add_argument("--delay", type=float, default=2.0, help="Seconds between Parfumo fetches")
    opts = ap.parse_args()

    scraper = engine.get_scraper()
    if opts.cases:
        return run_cases(scraper, opts.delay)
    if not opts.house or not opts.name:
        ap.error("provide --house and --name (or --cases)")
    v = resolve(scraper, opts.house, opts.name, opts.year,
                seeded_urls=opts.url, brand_crawl=opts.brand_crawl,
                max_pages=opts.max_pages, delay=opts.delay)
    print(f"{opts.house} / {opts.name!r} -> {v.decision} score={v.score:.3f} "
          f"runner_up={v.runner_up:.3f} via={v.discovery} scored={v.candidates_scored}")
    if v.record:
        print(f"  {v.record.url}")
        _print_record(v.record)
    return 0 if v.decision in ("accept", "manual_review") else 1


if __name__ == "__main__":
    raise SystemExit(main())
