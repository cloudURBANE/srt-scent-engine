"""High-precision fragrance launch-year resolution from Decodo SERPs.

This module deliberately ignores bare years.  A result is evidence only when an
explicit release verb and the exact fragrance name occur in the same sentence.
That keeps publication dates, house founding years, and sibling flankers from
becoming launch years.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable
from urllib.parse import urlparse


_SPACE_RE = re.compile(r"\s+")
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_SENTENCE_RE = re.compile(r"[^.!?\n]+(?:[.!?]|$)")
_RELEASE_RE = re.compile(
    r"(?i)\b(?:was\s+|is\s+)?(?:first\s+)?"
    r"(?:released|launched|introduced|debuted|created)\s+"
    r"(?:in|on|during)\s+(?P<year>(?:17|18|19|20)\d{2})\b"
)
_COPYRIGHT_RE = re.compile(r"(?i)\b(?:copyright|©|updated|published|posted)\b")
# Authoritative "no launch year exists" statement. A retailer omitting a date
# proves nothing, but Parfumo/Fragrantica explicitly printing "The release year
# is unknown" is positive evidence that no launch year is published anywhere.
_YEAR_UNKNOWN_RE = re.compile(
    r"\b(?:release|launch)\s+year\s+is\s+unknown\b"
    r"|\byear\s+of\s+(?:release|launch)\s+is\s+unknown\b",
    re.IGNORECASE,
)
# Canonical fragrance databases whose explicit "unknown" we trust as a negative.
_AUTHORITATIVE_YEAR_DOMAINS = ("parfumo.com", "fragrantica.com")


@dataclass(frozen=True)
class YearEvidence:
    year: int
    domain: str
    url: str
    sentence: str
    title: str


def _normal(text: Any) -> str:
    return " ".join(_TOKEN_RE.findall(str(text or "").lower()))


def _exact_name_in(text: str, name: str) -> bool:
    haystack = f" {_normal(text)} "
    needle = _normal(name)
    return bool(needle) and f" {needle} " in haystack


def _name_is_release_subject(sentence: str, name: str) -> bool:
    """Require the exact name to lead directly into the release predicate.

    A loose token check would bind ``Sauvage`` to ``Sauvage Elixir was released
    in 2021``. Requiring the predicate immediately after the requested name is
    what makes suffix flankers non-matches.
    """
    normalized = _normal(sentence)
    needle = _normal(name)
    if not normalized or not needle:
        return False
    return bool(
        re.search(
            rf"(?:^|\s){re.escape(needle)}\s+"
            r"(?:(?:was|is)\s+)?(?:first\s+)?"
            r"(?:released|launched|introduced|debuted|created)\s+"
            r"(?:in|on|during)\s+(?:17|18|19|20)\d{2}\b",
            normalized,
        )
    )


def _domain(url: str, fallback: str = "") -> str:
    host = (urlparse(url or "").hostname or fallback or "").lower().strip()
    return host[4:] if host.startswith("www.") else host


def extract_year_evidence(
    rows: Iterable[dict[str, Any]], brand: str, name: str, *, now_year: int | None = None
) -> list[YearEvidence]:
    """Extract exact-identity, explicit-launch statements from SERP rows."""
    ceiling = (now_year or datetime.now(timezone.utc).year) + 1
    identity_years = {int(y) for y in re.findall(r"\b(?:17|18|19|20)\d{2}\b", f"{brand} {name}")}
    found: list[YearEvidence] = []
    seen: set[tuple[int, str, str]] = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        title = str(row.get("title") or "").strip()
        snippet = str(row.get("snippet") or row.get("description") or row.get("desc") or "").strip()
        url = str(row.get("url_raw") or row.get("url") or row.get("link") or "").strip()
        domain = _domain(url, str(row.get("domain") or ""))
        for sentence_match in _SENTENCE_RE.finditer(snippet):
            sentence = _SPACE_RE.sub(" ", sentence_match.group(0)).strip()
            release = _RELEASE_RE.search(sentence)
            if not release or _COPYRIGHT_RE.search(sentence):
                continue
            year = int(release.group("year"))
            if year < 1700 or year > ceiling or year in identity_years:
                continue
            # The product name must be in the claim itself. Merely appearing in
            # the result title is insufficient because snippets often discuss a
            # sibling or comparison fragrance.
            if not _exact_name_in(sentence, name) or not _name_is_release_subject(sentence, name):
                continue
            # When the brand is stated in the claim, it must be the requested
            # brand. Brand omission is allowed (the exact product name remains).
            normalized_sentence = _normal(sentence)
            normalized_brand = _normal(brand)
            if normalized_brand and normalized_brand not in normalized_sentence:
                normalized_title = _normal(title)
                if normalized_brand not in normalized_title:
                    continue
            key = (year, domain, _normal(sentence))
            if key in seen:
                continue
            seen.add(key)
            found.append(YearEvidence(year, domain, url, sentence, title))
    return found


def choose_year(evidence: Iterable[YearEvidence]) -> dict[str, Any] | None:
    """Choose a year, refusing every conflict between explicit statements."""
    rows = list(evidence)
    years = {item.year for item in rows}
    if len(years) != 1:
        return None
    if not rows:
        return None
    year = rows[0].year
    domains = sorted({item.domain for item in rows if item.domain})
    confidence = 98 if len(domains) >= 2 else 95
    return {
        "year": str(year),
        "year_meta": {
            "confidence": confidence,
            "source": "decodo_serp_explicit_release",
            "domains": domains,
            "evidence_count": len(rows),
            "evidence": [
                {"url": item.url, "domain": item.domain, "sentence": item.sentence}
                for item in rows[:4]
            ],
            "resolved_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        },
    }


def _row_url(row: dict[str, Any]) -> str:
    return str(row.get("url_raw") or row.get("url") or row.get("link") or "").strip()


def _row_snippet(row: dict[str, Any]) -> str:
    return str(row.get("snippet") or row.get("description") or row.get("desc") or "").strip()


def _tokens(text: Any) -> set[str]:
    return set(_normal(text).split())


def _all_tokens_present(text: str, *parts: str) -> bool:
    """True when every token of each part appears somewhere in ``text`` (any
    order). Unlike :func:`_exact_name_in` this tolerates an interleaved brand —
    Parfumo/Fragrantica list "Ramz Lattafa Gold" for a "Lattafa / Ramz Gold"
    query — which is safe for the negative "year unknown" signal because no year
    is asserted; a sibling ("Ramz Silver") still fails on the missing token."""
    haystack = _tokens(text)
    needed: set[str] = set()
    for part in parts:
        needed |= _tokens(part)
    return bool(needed) and needed <= haystack


def _is_authoritative_domain(domain: str) -> bool:
    return any(
        domain == d or domain.endswith("." + d) for d in _AUTHORITATIVE_YEAR_DOMAINS
    )


def extract_year_unknown_signal(
    rows: Iterable[dict[str, Any]], brand: str, name: str
) -> dict[str, Any] | None:
    """Authoritative evidence that this fragrance's launch year is genuinely
    unpublished, or ``None``.

    Distinguishes "the resolver failed to find a year" from "no launch year
    exists" — the latter lets the worker mark the gap source-unsuppliable and
    stop re-billing Decodo for an undated fragrance forever. Requires an
    explicit "release year is unknown" statement on a canonical fragrance DB
    (Parfumo/Fragrantica) whose result is actually about the requested perfume.
    """
    matches: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        url = _row_url(row)
        domain = _domain(url, str(row.get("domain") or ""))
        if not _is_authoritative_domain(domain):
            continue
        snippet = _row_snippet(row)
        if not _YEAR_UNKNOWN_RE.search(snippet):
            continue
        title = str(row.get("title") or "")
        # The result must be about the requested fragrance, not a sibling. Use a
        # token-subset (name + brand) match so an interleaved brand spelling
        # ("Ramz Lattafa Gold" for "Lattafa / Ramz Gold") still binds.
        if not _all_tokens_present(f"{title} {snippet}", name, brand):
            continue
        if domain in seen:
            continue
        seen.add(domain)
        matches.append((domain, snippet, url))
    if not matches:
        return None
    return {
        "year": None,
        "unresolvable": True,
        "year_meta": {
            "confidence": 90,
            "source": "decodo_serp_authoritative_unknown",
            "domains": sorted({d for d, _, _ in matches}),
            "evidence_count": len(matches),
            "evidence": [
                {"url": u, "domain": d, "sentence": s} for d, s, u in matches[:4]
            ],
            "resolved_at": datetime.now(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z"),
        },
    }


def _payload_rows(client: Any, payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entry in client._search_entries(payload):
        if not isinstance(entry, dict):
            continue
        link = client._entry_link(entry)
        rows.append(
            {
                "title": entry.get("title") or "",
                "snippet": entry.get("snippet") or entry.get("description") or entry.get("desc") or "",
                "url_raw": link,
                "domain": _domain(link),
            }
        )
    return rows[:10]


def resolve_year_decodo(
    brand: str,
    name: str,
    *,
    client: Any | None = None,
    timeout: float = 15.0,
    logger: Callable[[str, Exception], None] | None = None,
) -> dict[str, Any] | None:
    """Resolve through Decodo Google, then Bing when corroboration is needed.

    Provider failures degrade to ``None``. The Decodo client's existing daily
    budget applies to both calls.
    """
    if client is None:
        from fragrance_parser_full_rewrite_fixed import DecodoScraperClient as client
    if not brand or not name or not client.enabled():
        return None
    query = f'"{brand} {name}" release date'
    evidence: list[YearEvidence] = []
    all_rows: list[dict[str, Any]] = []
    try:
        google_rows = _payload_rows(client, client._post_google(query, timeout))
        all_rows.extend(google_rows)
        evidence.extend(extract_year_evidence(google_rows, brand, name))
    except Exception as exc:
        (logger or client._log_request_failure)("year Google SERP", exc)
    google_choice = choose_year(evidence)
    google_domains = (google_choice or {}).get("year_meta", {}).get("domains", [])
    if len(google_domains) >= 2:
        return google_choice
    try:
        bing_rows = _payload_rows(client, client._post_bing(query, timeout))
        all_rows.extend(bing_rows)
        evidence.extend(extract_year_evidence(bing_rows, brand, name))
    except Exception as exc:
        (logger or client._log_request_failure)("year Bing SERP", exc)
    resolved = choose_year(evidence)
    if resolved:
        return resolved
    # No explicit launch year anywhere. Before giving up, check whether a
    # canonical fragrance DB explicitly says the year is unknown — that turns a
    # silent miss into a durable "source-unsuppliable" fact the worker can stop
    # re-billing Decodo for.
    return extract_year_unknown_signal(all_rows, brand, name)
