#!/usr/bin/env python3
"""
fragrance_parser_v35_fast_surgical.py
Fast dual-source fragrance parser with guarded autocomplete repair, strict identity
hard gates, visual-bar metric extraction, and semantic pyramid note extraction.
"""
from __future__ import annotations
import argparse
import html
from __future__ import annotations
import argparse
import html
import base64
import hashlib
import json
import os
import random
import re
import sys
import tempfile
import threading
import time
import unicodedata
from difflib import SequenceMatcher
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import quote, unquote, urljoin, urlparse

import requests

try:
    import cloudscraper  # pyright: ignore[reportMissingImports]
except ModuleNotFoundError:
    cloudscraper = None  # type: ignore[assignment]
try:
    from curl_cffi import requests as curl_requests  # pyright: ignore[reportMissingImports]
except ModuleNotFoundError:
    curl_requests = None  # type: ignore[assignment]
try:
    from DrissionPage import ChromiumOptions, ChromiumPage  # pyright: ignore[reportMissingImports]
except ModuleNotFoundError:
    ChromiumOptions = None  # type: ignore[assignment]
    ChromiumPage = None  # type: ignore[assignment]
from bs4 import BeautifulSoup

C = "\033[36m"
G = "\033[32m"
Y = "\033[33m"
R = "\033[31m"
Z = "\033[0m"
B = "\033[1m"
D = "\033[2m"
M = "\033[35m"

@dataclass
class Review:
    text: str
    source: str

@dataclass
class NotesList:
    has_pyramid: bool = False
    top: list[str] = field(default_factory=list)
    heart: list[str] = field(default_factory=list)
    base: list[str] = field(default_factory=list)
    flat: list[str] = field(default_factory=list)

@dataclass
class UnifiedDetails:
    notes: NotesList
    gender: str = "Unisex / Unspecified"
    description: str = ""
    bn_consensus: dict[str, tuple[int, int]] = field(default_factory=dict)
    frag_cards: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    pros_cons: list[str] = field(default_factory=list)
    reviews: list[Review] = field(default_factory=list)
    derived_metrics: dict[str, Any] | None = None

@dataclass
class UnifiedFragrance:
    name: str
    brand: str
    year: str
    bn_url: str = ""
    frag_url: str = ""
    bn_positive_pct: int = -1
    bn_vote_count: int = 0
    resolver_source: str = ""
    resolver_score: float = 0.0
    query_score: float = 0.0
    
    @property
    def identity(self) -> str:
        return f"{self.brand} {self.name}".strip()
        
    @property
    def cache_key(self) -> str:
        return IdentityTools.cache_key(self.brand, self.name, self.year)
        
    @property
    def token_set(self) -> set[str]:
        return IdentityTools.name_tokens(self.name, self.brand) | IdentityTools.brand_tokens(self.brand)

@dataclass
class CatalogItem:
    name: str
    brand: str
    year: str
    url: str
    
    @property
    def token_set(self) -> set[str]:
        return IdentityTools.name_tokens(self.name, self.brand) | IdentityTools.brand_tokens(self.brand)

class Deadline:
    def __init__(self, seconds: float | None):
        self.seconds = None if seconds is None else max(0.0, float(seconds))
        self.started = time.monotonic()
        self.ends_at = None if self.seconds is None else self.started + self.seconds
        
    @classmethod
    def from_absolute(cls, ends_at: float | None) -> "Deadline":
        obj = cls(None)
        obj.ends_at = ends_at
        obj.seconds = None if ends_at is None else max(0.0, ends_at - time.monotonic())
        obj.started = time.monotonic()
        return obj
        
    def remaining(self, default: float | None = None) -> float | None:
        if self.ends_at is None:
            return default
        return max(0.0, self.ends_at - time.monotonic())
        
    def timeout(self, cap: float, floor: float = 0.35) -> float:
        remaining = self.remaining(cap)
        if remaining is None:
            return cap
        return max(floor, min(float(cap), remaining))
        
    def expired(self) -> bool:
        remaining = self.remaining(None)
        return remaining is not None and remaining <= 0

class StageTimer:
    def __init__(self, enabled: bool = False):
        self.enabled = enabled
        self.total_start = time.monotonic()
        self.last = self.total_start
        
    def mark(self, label: str, skipped: bool = False) -> None:
        if not self.enabled:
            return
        now = time.monotonic()
        elapsed = now - self.last
        self.last = now
        status = "skipped" if skipped else f"{elapsed:.2f}s"
        print(f"{D}[TIME] {label}: {status}{Z}")
        
    def total(self, label: str = "total_search") -> None:
        if self.enabled:
            print(f"{D}[TIME] {label}: {time.monotonic() - self.total_start:.2f}s{Z}")

@dataclass
class ResolverTraceEvent:
    branch: str
    query: str = ""
    identity: str = ""
    brand: str = ""
    name: str = ""
    year: str = ""
    started_at: float = 0.0
    elapsed_ms: int = 0
    result_count: int = 0
    linked_count: int = 0
    skipped_reason: str = ""
    accepted_url: str = ""
    rejected_url: str = ""
    rejection_reason: str = ""
    remaining_budget_ms: int = 0

class ResolverTrace:
    def __init__(self, enabled: bool = False):
        self.enabled = bool(enabled)
        self.events: list[ResolverTraceEvent] = []
        self.started = time.monotonic()
        
    def start(self, branch: str, *, query: str = "", brand: str = "", name: str = "", year: str = "", deadline: Deadline | None = None) -> dict[str, Any]:
        return {
            "branch": branch,
            "query": TextSanitizer.clean(query),
            "brand": TextSanitizer.clean(brand),
            "name": TextSanitizer.clean(name),
            "year": TextSanitizer.clean(year),
            "started": time.monotonic(),
            "remaining": self._remaining_ms(deadline),
        }
        
    def finish(self, token: dict[str, Any], *, result_count: int = 0, linked_count: int = 0, skipped_reason: str = "", accepted_url: str = "", rejected_url: str = "", rejection_reason: str = "", deadline: Deadline | None = None) -> None:
        if not self.enabled:
            return
        started = float(token.get("started") or time.monotonic())
        brand = str(token.get("brand") or "")
        name = str(token.get("name") or "")
        self.events.append(ResolverTraceEvent(
            branch=str(token.get("branch") or ""),
            query=str(token.get("query") or ""),
            identity=f"{brand} | {name}".strip(" |"),
            brand=brand,
            name=name,
            year=str(token.get("year") or ""),
            started_at=started,
            elapsed_ms=int((time.monotonic() - started) * 1000),
            result_count=int(result_count or 0),
            linked_count=int(linked_count or 0),
            skipped_reason=skipped_reason,
            accepted_url=accepted_url,
            rejected_url=rejected_url,
            rejection_reason=rejection_reason,
            remaining_budget_ms=self._remaining_ms(deadline) if deadline else int(token.get("remaining") or 0),
        ))
        
    def add_skip(self, branch: str, reason: str, *, query: str = "", brand: str = "", name: str = "", year: str = "", deadline: Deadline | None = None) -> None:
        token = self.start(branch, query=query, brand=brand, name=name, year=year, deadline=deadline)
        self.finish(token, skipped_reason=reason, deadline=deadline)
        
    def add_reject(self, branch: str, url: str, reason: str, *, brand: str = "", name: str = "", year: str = "", deadline: Deadline | None = None) -> None:
        token = self.start(branch, brand=brand, name=name, year=year, deadline=deadline)
        self.finish(token, rejected_url=url, rejection_reason=reason, deadline=deadline)
        
    def print_summary(self, candidates: list[UnifiedFragrance] | None = None) -> None:
        if not self.enabled:
            return
        order = ["raw_search", "fg_cache", "designer_catalog", "related_page_crawl", "external_search", "browser_catalog"]
        grouped: dict[str, list[ResolverTraceEvent]] = {}
        for event in self.events:
            grouped.setdefault(event.branch, []).append(event)
        print(f"\n{D}[FG TRACE SUMMARY]{Z}")
        for branch in order:
            events = grouped.pop(branch, [])
            if not events:
                if branch in {"external_search", "browser_catalog"}:
                    print(f"{D}{branch}: skipped / disabled{Z}")
                continue
            linked = sum(e.linked_count for e in events)
            candidates_count = sum(e.result_count for e in events)
            elapsed = sum(e.elapsed_ms for e in events)
            reasons = [e.skipped_reason or e.rejection_reason for e in events if e.skipped_reason or e.rejection_reason]
            reason_text = f" / {', '.join(dict.fromkeys(reasons))}" if reasons else ""
            if all(e.skipped_reason for e in events) and linked == 0 and candidates_count == 0:
                print(f"{D}{branch}: skipped / {reasons[-1] if reasons else 'no_work'}{Z}")
            else:
                print(f"{D}{branch}: {linked} linked / {candidates_count} candidates / {elapsed}ms{reason_text}{Z}")
        for branch, events in sorted(grouped.items()):
            linked = sum(e.linked_count for e in events)
            candidates_count = sum(e.result_count for e in events)
            elapsed = sum(e.elapsed_ms for e in events)
            reasons = [e.skipped_reason or e.rejection_reason for e in events if e.skipped_reason or e.rejection_reason]
            reason_text = f" / {', '.join(dict.fromkeys(reasons))}" if reasons else ""
            print(f"{D}{branch}: {linked} linked / {candidates_count} candidates / {elapsed}ms{reason_text}{Z}")
        if candidates is not None:
            print(f"{D}total_fg_links: {sum(1 for item in candidates if item.frag_url)}{Z}")
        print(f"{D}total_elapsed: {int((time.monotonic() - self.started) * 1000)}ms{Z}\n")
        
    @staticmethod
    def _remaining_ms(deadline: Deadline | None) -> int:
        if not deadline:
            return 0
        remaining = deadline.remaining(0.0) or 0.0
        return int(max(0.0, remaining) * 1000)

class TextSanitizer:
    ZERO_WIDTH = re.compile(r"[\u200b\u200c\u200d\uFEFF]")
    SPACE = re.compile(r"\s+")
    YEAR_RE = re.compile(r"\b(?:17|18|19|20)\d{2}\b")
    CLEANUP_RULES = [
        (re.compile(r"(?i)proven[^a-z]*al\s+"), ""),
        (re.compile(r"(?i)ylang[^a-z]lang"), "ylang-ylang"),
        (re.compile(r"\ufffd"), ""),
        (re.compile(r"&#65533;"), ""),
        (re.compile(r"\?\(?\?\?\)?"), ""),
    ]
    
    @classmethod
    def clean(cls, text: object) -> str:
        if text is None:
            return ""
        value = html.unescape(str(text))
        for pattern, repl in cls.CLEANUP_RULES:
            value = pattern.sub(repl, value)
        value = cls.ZERO_WIDTH.sub("", value)
        value = cls.SPACE.sub(" ", value)
        return value.strip()
        
    @classmethod
    def normalize_identity(cls, text: object) -> str:
        value = cls.clean(text).lower()
        value = unicodedata.normalize("NFKD", value)
        value = "".join(ch for ch in value if not unicodedata.combining(ch))
        value = value.replace("&", " and ")
        value = re.sub(r"\bn\s*[°ºo]?\s*(\d+)\b", r"no \1", value)
        value = re.sub(r"\bno\s+(\d+)\b", r"no\1", value)
        value = re.sub(r"[^a-z0-9\s]", " ", value)
        value = cls.SPACE.sub(" ", value)
        return value.strip()
        
    @classmethod
    def title_from_slug(cls, slug: str) -> str:
        value = unquote(slug or "")
        value = re.sub(r"[-_]+", " ", value)
        value = cls.SPACE.sub(" ", value).strip()
        return value.title()

class IdentityTools:
    STOPWORDS = {
        "cologne", "perfume", "fragrance", "edt", "edp", "edc", "extrait",
        "parfum", "toilette", "eau", "de", "du", "la", "le", "l", "pour",
        "homme", "femme", "men", "women", "man", "woman", "male", "female",
        "unisex", "spray", "version", "original", "new", "intense",
    }
    BRAND_ALIASES = {
        "dior": {"dior", "christian dior"},
        "christian dior": {"dior", "christian dior"},
        "ysl": {"ysl", "yves saint laurent", "yves st laurent"},
        "yves saint laurent": {"ysl", "yves saint laurent", "yves st laurent"},
        "armani": {"armani", "giorgio armani"},
        "giorgio armani": {"armani", "giorgio armani"},
        "hermes": {"hermes", "hermès"},
        "hermès": {"hermes", "hermès"},
        "prada": {"prada"},
        "chanel": {"chanel"},
        "versace": {"versace"},
        "tom ford": {"tom ford", "ford"},
        "maison francis kurkdjian": {"maison francis kurkdjian", "mfk"},
        "mfk": {"maison francis kurkdjian", "mfk"},
        "jean paul gaultier": {"jean paul gaultier", "jpg"},
        "jpg": {"jean paul gaultier", "jpg"},
        "dolce and gabbana": {"dolce and gabbana", "d and g", "dg"},
    }
    
    @staticmethod
    def tokenized(text: str) -> set[str]:
        return IdentityTools.identity_tokens(TextSanitizer.normalize_identity(text))
        
    @staticmethod
    def brand_tokens(brand: str) -> set[str]:
        out: set[str] = set()
        for form in IdentityTools.brand_forms(brand):
            out |= IdentityTools.tokenized(form)
        return out
        
    @staticmethod
    def name_tokens(name: str, brand: str = "") -> set[str]:
        tokens = IdentityTools.tokenized(name)
        brand_tokens = IdentityTools.brand_tokens(brand) if brand else set()
        return {t for t in tokens if t not in brand_tokens}
        
    @staticmethod
    def identity_tokens(normalized: str) -> set[str]:
        return {token for token in normalized.split() if token and token not in IdentityTools.STOPWORDS}
        
    @staticmethod
    def cache_key(brand: str, name: str, year: str = "") -> str:
        brand_norm = TextSanitizer.normalize_identity(brand)
        name_norm = TextSanitizer.normalize_identity(name)
        year_norm = TextSanitizer.clean(year)
        return f"{brand_norm}|{name_norm}|{year_norm}"
        
    @staticmethod
    def brand_forms(brand: str) -> set[str]:
        normalized = TextSanitizer.normalize_identity(brand)
        forms = {normalized}
        forms |= IdentityTools.BRAND_ALIASES.get(normalized, set())
        return {item for item in forms if item}

    @staticmethod
    def canonical_brand_query(query: str) -> str:
        normalized = TextSanitizer.normalize_identity(query)
        if not normalized:
            return ""
        for canonical, forms in IdentityTools.BRAND_ALIASES.items():
            all_forms = {canonical} | set(forms)
            if normalized in all_forms:
                return canonical
        return normalized if normalized in IdentityTools.BRAND_ALIASES else ""
        
    @staticmethod
    def compatible_brand(a: str, b: str) -> bool:
        a_forms = IdentityTools.brand_forms(a)
        b_forms = IdentityTools.brand_forms(b)
        return bool(a_forms & b_forms) or TextSanitizer.normalize_identity(a) == TextSanitizer.normalize_identity(b)
        
    @staticmethod
    def token_overlap(a: set[str], b: set[str]) -> tuple[float, float]:
        if not a or not b:
            return 0.0, 0.0
        inter = len(a & b)
        return inter / len(a | b), inter / min(len(a), len(b))
        
    @staticmethod
    def display_tokens(text: str) -> set[str]:
        drop = {"cologne", "perfume", "fragrance", "spray", "version", "new", "by", "for", "men", "women", "male", "female", "unisex"}
        return {t for t in TextSanitizer.normalize_identity(text).split() if t and t not in drop}
        
    @staticmethod
    def query_name_tokens(query: str, brand: str) -> set[str]:
        tokens = IdentityTools.display_tokens(query)
        for form in IdentityTools.brand_forms(brand):
            ft = IdentityTools.display_tokens(form)
            if ft and ft.issubset(tokens):
                tokens -= ft
        return tokens or IdentityTools.display_tokens(query)
        
    @staticmethod
    def compact_phrase(text: str) -> str:
        return " ".join(TextSanitizer.normalize_identity(text).split())

    @staticmethod
    def seq_ratio(a: str, b: str) -> float:
        a = IdentityTools.compact_phrase(a)
        b = IdentityTools.compact_phrase(b)
        if not a or not b:
            return 0.0
        return SequenceMatcher(None, a, b).ratio()

    @staticmethod
    def fuzzy_token_coverage(query_tokens: set[str], target_tokens: set[str]) -> float:
        if not query_tokens or not target_tokens:
            return 0.0
        scores: list[float] = []
        for q in query_tokens:
            best = 0.0
            for t in target_tokens:
                if q == t:
                    best = max(best, 1.0)
                elif len(q) >= 4 and (t.startswith(q) or q.startswith(t)):
                    best = max(best, 0.92)
                elif len(q) >= 4 and len(t) >= 4:
                    best = max(best, SequenceMatcher(None, q, t).ratio())
            scores.append(best)
        return sum(scores) / len(scores)

    @staticmethod
    def fused_identity_score(query: str, name: str, brand: str) -> float:
        q = IdentityTools.compact_phrase(query)
        if not q:
            return 0.0
        name_norm = IdentityTools.compact_phrase(name)
        brand_forms = IdentityTools.brand_forms(brand) or {IdentityTools.compact_phrase(brand)}
        phrases = {name_norm}
        for bf in brand_forms:
            bf = IdentityTools.compact_phrase(bf)
            if bf and name_norm:
                phrases.add(f"{bf} {name_norm}")
                phrases.add(f"{name_norm} {bf}")
        return max((IdentityTools.seq_ratio(q, phrase) for phrase in phrases if phrase), default=0.0)

    @staticmethod
    def relevance_score(query: str, item: "UnifiedFragrance") -> float:
        query_clean = TextSanitizer.clean(query).lower()
        name_clean = TextSanitizer.clean(item.name).lower()
        brand_clean = TextSanitizer.clean(item.brand).lower()
        target_full = f"{brand_clean} {name_clean}".strip()
        
        q_tokens = query_clean.split()
        target_words = target_full.split()

        if not q_tokens or not target_words:
            return 0.0

        matched_score = 0.0
        for qt in q_tokens:
            if qt in target_words or qt in target_full:
                matched_score += 1.0
            else:
                best_word_match = max([SequenceMatcher(None, qt, t_word).ratio() for t_word in target_words] + [0.0])
                matched_score += best_word_match

        token_confidence = matched_score / max(1, len(q_tokens))
        raw_ratio = max(
            SequenceMatcher(None, query_clean, name_clean).ratio(),
            SequenceMatcher(None, query_clean, brand_clean).ratio(),
            SequenceMatcher(None, query_clean, target_full).ratio()
        )

        return max(token_confidence, raw_ratio)

class QueryRepair:
    _LAST_NATIVE_CANDIDATES: list = []
    MIN_USEFUL_TOP_SCORE = 0.72
    MIN_BN_STRONG_SCORE = 0.72
    MIN_DIRECT_SCORE = 0.74
    MIN_REPAIR_CONFIDENCE = 0.86
    MIN_TITLE_ONLY_CONFIDENCE = 0.96
    MAX_SUGGESTION_WORDS = 8
    MAX_CANONICAL_WORDS = 12

    BAD_SUGGESTION_PARTS = {
        # search/result UI
        "search", "searched", "searches", "result", "results",
        "image", "images", "video", "videos", "news", "map", "maps",
        "shopping", "shop", "shops", "web", "website", "page", "pages",

        # account/browser/cache UI
        "login", "signin", "sign", "cached", "cache", "translate", "translation",
        "translated", "preview", "snippet", "redirect", "url", "link", "links",

        # source/platform noise
        "wikipedia", "youtube", "reddit", "tiktok", "facebook", "instagram",
        "pinterest", "twitter", "x", "threads", "quora", "medium",

        # shopping/retail noise
        "amazon", "walmart", "ebay", "etsy", "sephora", "ulta", "macys",
        "macy", "nordstrom", "fragrancenet", "fragrancex", "microperfumes",
        "scentbird", "scentbox", "luckyscent", "jomashop",

        # commercial/action noise
        "buy", "sale", "sell", "sold", "discount", "discounts", "coupon",
        "coupons", "deal", "deals", "offer", "offers", "price", "prices",
        "pricing", "cheap", "cheapest", "shipping", "delivery", "cart",

        # page metadata tails
        "description", "descriptions", "overview", "summary", "details",
        "information", "info", "profile", "profiles", "article", "articles",
        "blog", "blogs", "guide", "guides",

        # review/rating page tails
        "review", "reviews", "rating", "ratings", "rated", "rank", "ranking",
        "rankings", "score", "scores", "vote", "votes",

        # fragrance-page section labels that should not survive as query text
        "notes", "accords", "ingredients", "pyramid", "longevity", "sillage",
        "projection", "season", "seasons", "weather", "occasion", "occasions",

        # clone/comparison intent
        "dupe", "dupes", "clone", "clones", "alternative", "alternatives",
        "similar", "comparison", "compare", "versus", "vs",

        # generic bad exact UI pieces sometimes parsed as candidates
        "all", "more", "next", "previous", "back", "home", "menu",
    }

    BAD_EXACT_SUGGESTIONS = {
        # click/navigation UI
        "click here", "here", "learn more", "read more", "show more",
        "see more", "view more", "load more", "more results",

        # search UI
        "search", "search results", "all results", "results",
        "images", "image results", "videos", "video results",
        "news", "maps", "shopping", "web results",

        # account/cache/translation UI
        "sign in", "login", "log in", "cached", "cache",
        "translate this page", "translated page",

        # bad generic labels
        "all", "more", "next", "previous", "back", "home", "menu",
        "official site", "website", "page", "result",

        # page section labels
        "description", "reviews", "rating", "ratings", "notes",
        "accords", "ingredients", "longevity", "sillage",
        "projection", "price", "prices",
    }

    GENERIC_TOKENS = {
        "fragrance", "fragrances", "perfume", "perfumes", "cologne", "colognes",
        "scent", "scents", "review", "reviews", "rating", "ratings", "price",
        "prices", "buy", "sale", "sample", "samples", "spray", "official", "site",
        "page", "result", "results", "men", "women", "unisex", "male", "female",
        "for", "and", "or", "the", "a", "an", "by", "of", "to", "with",
        "eau", "de", "du", "la", "le", "el", "parfum", "parfums", "perfume",
        "toilette", "cologne", "pour", "homme", "femme", "intense", "edt", "edp",
        "extrait", "absolu", "absolute", "tester", "new", "old",
    }

    CUT_AFTER_RE = re.compile(
    r"(?i)\b("
    # question/search intent
    r"which|what|who|where|when|why|how|best|better|top|list|"
    r"compare|comparison|vs|versus|similar|smells like|"
    # review/info tails
    r"review|reviews|rating|ratings|rated|description|descriptions|"
    r"overview|summary|details|info|information|profile|"
    r"notes|accords|ingredients|pyramid|longevity|sillage|projection|"
    r"season|seasons|weather|occasion|occasions|"
    # shopping/price tails
    r"price|prices|pricing|buy|sale|sell|shop|shopping|discount|"
    r"coupon|coupons|deal|deals|sample|samples|decant|decants|tester|"
    r"amazon|walmart|ebay|etsy|sephora|ulta|macys|macy's|nordstrom|"
    r"fragrancenet|fragrancex|microperfumes|scentbird|scentbox|"
    # platform/domain tails
    r"reddit|tiktok|youtube|facebook|instagram|pinterest|twitter|"
    r"fragrantica|basenotes|parfumo|wikipedia|"
    # bad result/UI tails
    r"search|result|results|image|images|video|videos|news|map|maps|"
    r"cached|cache|translate|translation|login|sign in|official site|"
    # clone/dupe tails
    r"dupe|dupes|clone|clones|alternative|alternatives"
    r")\b.*$"
    )

    TITLE_TAIL_RE = re.compile(
    r"(?i)\b("
    # generic page/category words
    r"colognes?|perfumes?|fragrances?|scents?|"
    r"reviews?|ratings?|rated|descriptions?|overview|summary|details|"
    r"info|information|profile|"
    # fragrance-page sections
    r"notes?|accords?|ingredients?|pyramid|longevity|sillage|projection|"
    r"season|seasons|weather|occasion|occasions|"
    # shopping tails
    r"prices?|pricing|buy|sale|shop|shopping|discount|coupons?|deals?|"
    r"samples?|decants?|tester|"
    # gender tails
    r"for men|for women|for women and men|for men and women|"
    r"men's|womens?|unisex|"
    # source/site tails
    r"fragrantica|basenotes|parfumo|wikipedia|reddit|youtube|tiktok|"
    r"amazon|walmart|ebay|etsy|sephora|ulta|macys|macy's|nordstrom|"
    r"fragrancenet|fragrancex|microperfumes|scentbird|scentbox"
    r")\b.*$"
    )

    BLOCKED_URL_PARTS = (
        "google.com/search", "bing.com/search", "webcache", "translate.google",
        "/search?", "/images?", "/videos?", "/news?", "/maps?", "/shopping?",
        "tbm=isch", "tbm=nws", "udm=2", "/imgres?", "/aclk?", "/url?sa=u",
    )

    BLOCKED_DOMAINS = (
        "youtube.", "youtu.be", "reddit.", "tiktok.", "facebook.", "instagram.",
        "pinterest.", "twitter.", "x.com", "wikipedia.", "amazon.", "walmart.",
        "ebay.", "etsy.", "sephora.", "ulta.", "macys.", "nordstrom.", "fragrancex.",
        "fragrancenet.", "microperfumes.", "scentbird.", "scentbox.",
    )

    CANONICAL_DOMAIN_HINTS = ("fragrantica.", "basenotes.", "parfumo.")


    @staticmethod
    def _anchor_fallback_query(query: str) -> str:
        tokens = QueryRepair._tokens(query)

        if len(tokens) < 3:
            return ""

        original_key = QueryRepair._identity(query)

        # Case 1:
        # Short final token may be the actual fragrance name.
        # "sant laurent y" should not become "sant laurent".
        # Safer fallback: "laurent y".
        if len(tokens[-1]) <= 2 and len(tokens[-2]) >= 3:
            anchor = f"{tokens[-2]} {tokens[-1]}"
            if QueryRepair._identity(anchor) != original_key:
                return QueryRepair.normalized_query(anchor)

        # Case 2:
        # First token looks damaged/short and the last token may be the brand.
        # "frd ombre leather tom" should not become "frd ombre leather".
        # Safer fallback: "ombre leather tom".
        if len(tokens) >= 4 and len(tokens[0]) <= 3 and len(tokens[-1]) <= 3:
            anchor = " ".join(tokens[1:])
            if QueryRepair._identity(anchor) != original_key:
                return QueryRepair.normalized_query(anchor)

        anchors: list[tuple[str, int, int]] = []
        seen: set[str] = set()

        def add_anchor(anchor_tokens: list[str], index: int, width: int) -> None:
            if len(anchor_tokens) < 2:
                return

            if any(len(token) < 3 for token in anchor_tokens):
                return

            anchor = " ".join(anchor_tokens)
            key = QueryRepair._identity(anchor)

            if not key or key in seen:
                return

            if key == original_key:
                return

            seen.add(key)
            anchors.append((anchor, index, width))

        if len(tokens) >= 4:
            for i in range(len(tokens) - 2):
                add_anchor(tokens[i:i + 3], i, 3)

        for i in range(len(tokens) - 1):
            add_anchor(tokens[i:i + 2], i, 2)

        if not anchors:
            return ""

        def anchor_rank(item: tuple[str, int, int]) -> tuple[int, int, int, int, int, int]:
            anchor, index, width = item
            anchor_tokens = anchor.split()

            preserves_first_token = 1 if index == 0 else 0
            ends_at_tail = 1 if index + width == len(tokens) else 0
            strong_len_tokens = sum(1 for token in anchor_tokens if len(token) >= 5)
            medium_len_tokens = sum(1 for token in anchor_tokens if len(token) >= 4)
            total_len = sum(len(token) for token in anchor_tokens)

            return (
                preserves_first_token,
                -ends_at_tail,
                width,
                strong_len_tokens,
                medium_len_tokens,
                total_len,
            )

        anchors.sort(key=anchor_rank, reverse=True)

        best = anchors[0][0]

        if QueryRepair._identity(best) == original_key:
            return ""

        return QueryRepair.normalized_query(best)
    
    @staticmethod
    def _question_intent_cleanup(query: str) -> str:
        original = QueryRepair.normalized_query(query)

        if not original:
            return ""

        words = QueryRepair._identity(original).split()

        if not words:
            return ""

        question_openers = {
            "does", "do", "did", "is", "are", "was", "were",
            "can", "could", "should", "would", "will",
            "what", "which", "why", "how", "where", "when",
        }

        # Only activate when the query clearly starts like a question.
        if words[0] not in question_openers:
           return ""

        cleaned = re.sub(
            r"(?i)^(does|do|did|is|are|was|were|can|could|should|would|will|what|which|why|how|where|when)\s+",
            "",
            original,
        )

        # Cut after intent words that are not part of fragrance identity.
        # Example:
        # "dior sauvage smell good" -> "dior sauvage"
        cleaned = re.sub(
            r"(?i)\b(smell|smells|smelling|last|lasting|perform|performs|performance|project|projects|projection|worth|compliment|compliments)\b.*$",
            "",
            cleaned,
        )

        cleaned = QueryRepair.clean_suggestion(cleaned)

        if not cleaned:
            return ""

        cleaned_tokens = QueryRepair._tokens(cleaned)

        # Do not collapse to one vague token.
        if len(cleaned_tokens) < 2:
            return ""

        if QueryRepair._identity(cleaned) == QueryRepair._identity(original):
            return ""

        return QueryRepair.normalized_query(cleaned)
    
    
    @staticmethod
    def _preserves_short_or_tail_token(original: str, suggestion: str) -> bool:
        original_tokens = QueryRepair._tokens(original)
        suggestion_tokens = QueryRepair._tokens(suggestion)

        if len(original_tokens) < 3 or not suggestion_tokens:
            return True

        protected: list[str] = []

        # Short final tokens can be real fragrance identity:
        # "sant laurent y" -> must not become "sant laurent"
        if len(original_tokens[-1]) <= 2:
            protected.append(original_tokens[-1])

        # Short tail brand fragments should not be silently dropped:
        # "frd ombre leather tom" -> must not become "frd ombre leather"
        if len(original_tokens) >= 4 and len(original_tokens[-1]) <= 3:
            protected.append(original_tokens[-1])

        if not protected:
            return True

        for token in protected:
            if not any(QueryRepair._token_score(token, cand) >= 0.90 for cand in suggestion_tokens):
                return False

        return True
    
    
    
    
    
    
    @staticmethod
    def _preserves_first_meaningful_token(original: str, suggestion: str) -> bool:
        original_tokens = QueryRepair._tokens(original)
        suggestion_tokens = QueryRepair._tokens(suggestion)

        if not original_tokens or not suggestion_tokens:
            return False

        first = original_tokens[0]

        # The first meaningful token is usually the brand or strongest anchor.
        # It may appear anywhere in the suggestion, especially for reversed input.
        return any(
            QueryRepair._token_score(first, cand) >= 0.90
            for cand in suggestion_tokens
        )    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    @staticmethod
    def _meaningful_token_bag(text: str) -> list[str]:
        return sorted(QueryRepair._tokens(text))

    @staticmethod
    def _is_reorder_only(original: str, suggestion: str) -> bool:
        original_tokens = QueryRepair._meaningful_token_bag(original)
        suggestion_tokens = QueryRepair._meaningful_token_bag(suggestion)

        if not original_tokens or not suggestion_tokens:
            return False

        return original_tokens == suggestion_tokens and QueryRepair._identity(original) != QueryRepair._identity(suggestion)

    @staticmethod
    def _adds_or_repairs_meaningful_token(original: str, suggestion: str) -> bool:
        original_tokens = QueryRepair._tokens(original)
        suggestion_tokens = QueryRepair._tokens(suggestion)

        if not original_tokens or not suggestion_tokens:
            return False

        if QueryRepair._is_reorder_only(original, suggestion):
            return False

        for src in original_tokens:
            best = max((QueryRepair._token_score(src, cand) for cand in suggestion_tokens), default=0.0)

            if best < QueryRepair.MIN_DIRECT_SCORE:
                return False

        original_set = set(original_tokens)
        suggestion_set = set(suggestion_tokens)

        if suggestion_set - original_set:
            return True

        for src in original_tokens:
            for cand in suggestion_tokens:
                if src != cand and QueryRepair._token_score(src, cand) >= QueryRepair.MIN_DIRECT_SCORE:
                    return True

        return False
    

    @staticmethod
    def needs_repair(bn_results: list[UnifiedFragrance], candidates: list[UnifiedFragrance]) -> bool:
        bn_results = bn_results or []
        candidates = candidates or []
   
        # Huge fix:
        # Cache the weak native candidates so suggest() can repair from the actual
        # Fragrantica result pool before wasting time on Google/Bing.
        QueryRepair._LAST_NATIVE_CANDIDATES = list(candidates[:40])

        if not bn_results and not candidates:
          return True

        bn_top = max((QueryRepair._score01(getattr(item, "query_score", 0.0)) for item in bn_results), default=0.0)
        all_top = max(
            (QueryRepair._score01(getattr(item, "query_score", 0.0)) for item in [*bn_results, *candidates]),
            default=0.0,
        )

        if bn_top >= QueryRepair.MIN_BN_STRONG_SCORE:
            return False

        if all_top >= 0.78:
            return False

        return all_top < QueryRepair.MIN_USEFUL_TOP_SCORE

    @staticmethod
    def normalized_query(text: str) -> str:
        text = TextSanitizer.clean(text)
        text = re.sub(r"(?i)\b(fragrance|fragrances|perfume|perfumes|cologne|colognes|scent|scents)\b", " ", text)
        text = re.sub(r"[?!.:;|•·]+", " ", text)
        return TextSanitizer.SPACE.sub(" ", text).strip()



    @staticmethod
    def _native_candidate_text(item) -> str:
        parts: list[str] = []

        for attr in (
            "house",
            "brand",
            "designer",
            "name",
            "title",
            "fragrance",
            "display_name",
            "label",
        ):
            value = getattr(item, attr, "")
            if value:
                value = QueryRepair.clean_suggestion(str(value))
                if value and QueryRepair._identity(value) not in {QueryRepair._identity(p) for p in parts}:
                    parts.append(value)

        # Prefer "House Name" when available.
        if len(parts) >= 2:
            return QueryRepair.clean_suggestion(f"{parts[0]} {parts[-1]}")

        if parts:
            return QueryRepair.clean_suggestion(parts[0])

        return ""
  

    @staticmethod
    def _native_candidate_url(item) -> str:
        direct_attrs = (
            "url", "link", "href", "source_url", "detail_url",
            "fg_url", "fragrantica_url", "bn_url", "basenotes_url",
        )

        for attr in direct_attrs:
            value = getattr(item, attr, "")
            if value:
                return str(value)

        # Fallback: inspect every simple string field for embedded fragrance URLs.
        try:
            values = vars(item).values()
        except Exception:
            values = []

        for value in values:
            if not isinstance(value, str):
                continue

            value = html.unescape(value).strip()

            if "/perfume/" in value or "/fragrances/" in value or "/Perfumes/" in value:
                match = re.search(
                    r"(https?://[^\s\"'<>]+|/[^\s\"'<>]*(?:perfume|fragrances|Perfumes)[^\s\"'<>]+)",
                    value,
                    re.I,
                )
                if match:
                    return match.group(1)

        return ""


    @staticmethod
    def _seed_records_from_native_candidates(records: dict, original: str) -> None:
        for item in QueryRepair._LAST_NATIVE_CANDIDATES[:40]:
            url = QueryRepair._native_candidate_url(item)
            print("[QR DEBUG native url]", url)
            candidate, domain, is_canonical = QueryRepair._canonical_candidate_from_url(url)
            print("[QR DEBUG parsed]", candidate, domain, is_canonical)
            if is_canonical and candidate:
                QueryRepair._add_candidate(records, candidate, source=domain or "native_fg", canonical=True)
                continue

            candidate = QueryRepair._native_candidate_text(item)

            if candidate:
                # Treat native FG rows as stronger than title text, but still force scoring.
                # This lets "Creed Irish Wata" resolve if "Creed Green Irish Tweed"
                # exists in the 25 native FG rows.
                QueryRepair._add_candidate(records, candidate, source="native_fg", canonical=True)



    @staticmethod
    def _score01(value) -> float:
        try:
            score = float(value or 0.0)
        except Exception:
            return 0.0

        if score > 1.0:
            score = score / 100.0

        if score < 0.0:
            return 0.0

        if score > 1.0:
            return 1.0

        return score

    @staticmethod
    def _identity(text: str) -> str:
        return TextSanitizer.normalize_identity(QueryRepair.normalized_query(text))

    @staticmethod
    def _tokens(text: str) -> list[str]:
        norm = QueryRepair._identity(text)
        return [t for t in norm.split() if t and t not in QueryRepair.GENERIC_TOKENS]

    @staticmethod
    def _safe_unquote(text: str) -> str:
        try:
            from urllib.parse import unquote_plus
            return unquote_plus(text or "")
        except Exception:
            return (text or "").replace("+", " ")

    @staticmethod
    def _slug_to_words(text: str) -> str:
        text = QueryRepair._safe_unquote(text)
        text = re.sub(r"[_\-]+", " ", text)
        text = re.sub(r"\.\d+.*$", "", text)
        text = re.sub(r"\b\d{4,}\b", " ", text)
        return QueryRepair.clean_suggestion(text)

    @staticmethod
    def _edit_distance(a: str, b: str, limit: int | None = None) -> int:
        a = (a or "")[:64]
        b = (b or "")[:64]

        if a == b:
            return 0

        if limit is not None and abs(len(a) - len(b)) > limit:
            return limit + 1

        if len(a) > len(b):
            a, b = b, a

        prev = list(range(len(b) + 1))

        for i, ca in enumerate(a, 1):
            cur = [i]
            row_min = i

            for j, cb in enumerate(b, 1):
                val = min(
                    prev[j] + 1,
                    cur[j - 1] + 1,
                    prev[j - 1] + (ca != cb),
                )
                cur.append(val)
                row_min = min(row_min, val)

            if limit is not None and row_min > limit:
                return limit + 1

            prev = cur

        return prev[-1]

    @staticmethod
    def _edit_distance_lte(a: str, b: str, limit: int = 3) -> bool:
        return QueryRepair._edit_distance(a, b, limit) <= limit

    @staticmethod
    def _one_adjacent_swap(a: str, b: str) -> bool:
        if len(a) != len(b) or a == b:
            return False

        diffs = [i for i, (ca, cb) in enumerate(zip(a, b)) if ca != cb]

        if len(diffs) != 2:
            return False

        i, j = diffs
        return j == i + 1 and a[i] == b[j] and a[j] == b[i]

    @staticmethod
    def _subsequence_ratio(short: str, long: str) -> float:
        if not short or not long:
            return 0.0

        pos = 0
        hit = 0

        for ch in short:
            found = long.find(ch, pos)
            if found < 0:
                continue
            hit += 1
            pos = found + 1

        return hit / max(1, len(short))


    @staticmethod
    def _two_token_repair_allowed(original: str, suggestion: str) -> bool:
        original_tokens = QueryRepair._tokens(original)
        suggestion_tokens = QueryRepair._tokens(suggestion)

        if len(original_tokens) != 2 or len(suggestion_tokens) < 2:
            return False

        first = original_tokens[0]
        second = original_tokens[1]

        # First token should lock onto brand or a real candidate token.
        first_best = max((QueryRepair._token_score(first, cand) for cand in suggestion_tokens), default=0.0)

        if first_best < 0.90:
            return False

        second_scores = [
            QueryRepair._token_score(second, cand)
            for cand in suggestion_tokens
            if cand != first
        ]

        second_best = max(second_scores, default=0.0)

        if second_best >= 0.70:
            return True

        # Extra rescue for cases like:
        # "sava" -> "sauvage"
        # "sauvag" -> "sauvage"
        # "savauge" -> "sauvage"
        for cand in suggestion_tokens:
            if cand == first or len(second) < 4 or len(cand) < 5:
                continue

            compact_second = re.sub(r"[aeiouy]+", "", second)
            compact_cand = re.sub(r"[aeiouy]+", "", cand)

            if compact_second and compact_cand.startswith(compact_second[:3]):
                return True

            if QueryRepair._subsequence_ratio(second, cand) >= 0.70 and second[0] == cand[0]:
                return True

        return False




    @staticmethod
    def _token_score(original_token: str, candidate_token: str) -> float:
        a = original_token or ""
        b = candidate_token or ""

        if not a or not b:
            return 0.0

        if a == b:
            return 1.0

        if len(a) < 3 or len(b) < 3:
            return 0.0

        if QueryRepair._one_adjacent_swap(a, b):
            return 0.86

        small = a if len(a) <= len(b) else b
        large = b if len(a) <= len(b) else a

        if large.startswith(small):
            ratio = len(small) / max(1, len(large))
            return 0.88 + (0.10 * ratio)

        if small in large and len(small) >= 4:
            return 0.82

        max_len = max(len(a), len(b))
        min_len = min(len(a), len(b))

        if min_len >= 4 and max_len >= 7:
            dist_limit = 3
        elif min_len >= 5:
            dist_limit = 2
        else:
            dist_limit = 1

        dist = QueryRepair._edit_distance(a, b, dist_limit)

        if dist <= dist_limit:
            ratio = 1.0 - (dist / max_len)
            if ratio >= 0.55:
                return max(0.74, ratio)

        if min_len >= 4 and a[0] == b[0]:
            subseq = QueryRepair._subsequence_ratio(small, large)
            if subseq >= 0.75:
                return 0.76

        return 0.0

    @staticmethod
    def _match_scores(original_tokens: list[str], suggestion_tokens: list[str]) -> list[tuple[str, str, float]]:
        out: list[tuple[str, str, float]] = []

        for src in original_tokens:
            best_token = ""
            best_score = 0.0

            for cand in suggestion_tokens:
                score = QueryRepair._token_score(src, cand)
                if score > best_score:
                    best_token = cand
                    best_score = score

            out.append((src, best_token, best_score))

        return out

    @staticmethod
    def _candidate_score(original: str, suggestion: str, allow_long: bool = False) -> float:
        original_norm = QueryRepair._identity(original)
        suggestion_norm = QueryRepair._identity(suggestion)

        if not original_norm or not suggestion_norm or original_norm == suggestion_norm:
            return 0.0

        if suggestion_norm in QueryRepair.BAD_EXACT_SUGGESTIONS:
            return 0.0

        suggestion_words = suggestion_norm.split()
        max_words = QueryRepair.MAX_CANONICAL_WORDS if allow_long else QueryRepair.MAX_SUGGESTION_WORDS

        if len(suggestion_words) > max_words or len(suggestion_norm) < 3:
            return 0.0

        if any(part in suggestion_words for part in QueryRepair.BAD_SUGGESTION_PARTS):
            return 0.0

        original_tokens = QueryRepair._tokens(original_norm)
        suggestion_tokens = QueryRepair._tokens(suggestion_norm)

        if not original_tokens or not suggestion_tokens:
            return 0.0

        if len(original_tokens) == 1:
            token = original_tokens[0]
            scored = [(s, QueryRepair._token_score(token, s)) for s in suggestion_tokens]
            best_token, best = max(scored, key=lambda item: item[1], default=("", 0.0))

            if best < QueryRepair.MIN_DIRECT_SCORE:
                return 0.0

            if best_token == token and len(suggestion_tokens) > 1:
                return 0.0

            return best

        matches = QueryRepair._match_scores(original_tokens, suggestion_tokens)
        scores = [score for _, _, score in matches]
        strong = [score for score in scores if score >= QueryRepair.MIN_DIRECT_SCORE]

        if len(original_tokens) == 2 and QueryRepair._two_token_repair_allowed(original_norm, suggestion_norm):
            return max(0.78, sum(scores) / max(1, len(scores)))


        if len(strong) < 2:
            return 0.0

        # Canonical URL evidence gets one-token forgiveness.
        # Example:
        # "creed irish wata"   -> "Creed Green Irish Tweed"
        # "creed irish tower"  -> "Creed Green Irish Tweed"
        #
        # This is only allowed when allow_long=True, which your class uses for
        # canonical fragrance URLs. It does NOT make autocomplete/title-only
        # guesses loose.
        if allow_long and len(original_tokens) >= 3 and len(strong) >= 2:
            weak = [score for score in scores if score < 0.62]

            if len(weak) <= 1:
                return max(0.76, sum(scores) / max(1, len(scores)))

        if min(scores, default=0.0) < 0.62:
            return 0.0

    @staticmethod
    def suggestion_is_valid(original: str, suggestion: str) -> bool:
        try:
            score = float(QueryRepair._candidate_score(original, suggestion) or 0.0)
        except Exception:
            return False

        return score >= QueryRepair.MIN_DIRECT_SCORE

    @staticmethod
    def clean_suggestion(text: str) -> str:
        text = TextSanitizer.clean(text)
        text = re.sub(r"(?i)<[^>]+>", " ", text)
        text = re.sub(r"(?i)^(showing results for|search instead for|did you mean:?|including results for)\s*", "", text)
        text = re.sub(r"(?i)\b(search instead for|showing results for|including results for|search only for)\b.*$", "", text)
        text = QueryRepair.CUT_AFTER_RE.sub("", text)
        text = re.sub(r"(?i)\b(fragrance|fragrances|perfume|perfumes|cologne|colognes|scent|scents)\b", " ", text)
        text = re.sub(r"[?!.:;|•·]+", " ", text)
        text = re.sub(r"\s+[-–—/]\s*$", "", text)
        text = text.strip(" -–—/\t\n\r")
        return TextSanitizer.SPACE.sub(" ", text).strip()

    @staticmethod
    def _unwrap_result_url(href: str) -> str:
        href = html.unescape(href or "").strip().replace("\\/", "/")

        if not href:
            return ""

        try:
            from urllib.parse import parse_qs, unquote, urlparse

            parsed = urlparse(href)
            query = parse_qs(parsed.query)
    
            for key in ("q", "url", "u", "uddg"):
                value = query.get(key, [""])[0]
                if value.startswith("http"):
                    return unquote(value)

            href = unquote(href)

            # Critical fix:
            # Native Fragrantica/Basenotes/Parfumo rows may store relative URLs.
            if href.startswith("//"):
                return "https:" + href

            if href.startswith("/perfume/"):
                return "https://www.fragrantica.com" + href

            if href.startswith("/fragrances/"):
                return "https://basenotes.com" + href

            if href.startswith("/Perfumes/"):
                return "https://www.parfumo.com" + href

            if href.startswith("http"):
                return href

            return href
        except Exception:
            if href.startswith("/perfume/"):
                return "https://www.fragrantica.com" + href
            if href.startswith("/fragrances/"):
                return "https://basenotes.com" + href
            if href.startswith("/Perfumes/"):
                return "https://www.parfumo.com" + href
            return href

    @staticmethod
    def _domain_from_url(url: str) -> str:
        try:
            from urllib.parse import urlparse
            return (urlparse(url).netloc or "").lower().removeprefix("www.")
        except Exception:
            return ""

    @staticmethod
    def _blocked_url(url: str) -> bool:
        low = (url or "").lower()

        if not low:
            return True

        if any(part in low for part in QueryRepair.BLOCKED_URL_PARTS):
            return True

        domain = QueryRepair._domain_from_url(low)
        return any(blocked in domain for blocked in QueryRepair.BLOCKED_DOMAINS)

    @staticmethod
    def _canonical_candidate_from_url(href: str) -> tuple[str, str, bool]:
        url = QueryRepair._unwrap_result_url(href)
        low = url.lower()

        if not url or not low.startswith("http") or QueryRepair._blocked_url(url):
            return "", "", False

        domain = QueryRepair._domain_from_url(url)
        clean_url = url.split("#", 1)[0].split("?", 1)[0]

        if "fragrantica." in low:
            match = re.search(r"/perfume/([^/?#]+)/([^/?#]+?)(?:-\d+)?\.html$", clean_url, re.I)
            if match:
                brand = QueryRepair._slug_to_words(match.group(1))
                name = QueryRepair._slug_to_words(re.sub(r"-\d+$", "", match.group(2)))
                candidate = QueryRepair.clean_suggestion(f"{brand} {name}")
                return candidate, domain or "fragrantica", bool(candidate)

        if "basenotes." in low:
            match = re.search(r"/fragrances/([^/?#]+)", clean_url, re.I)
            if match:
                slug = QueryRepair._safe_unquote(match.group(1))
                slug = re.sub(r"\.\d+.*$", "", slug)

                if "-by-" in slug:
                    name, brand = slug.split("-by-", 1)
                    candidate = QueryRepair.clean_suggestion(
                        f"{QueryRepair._slug_to_words(brand)} {QueryRepair._slug_to_words(name)}"
                    )
                    return candidate, domain or "basenotes", bool(candidate)

                candidate = QueryRepair._slug_to_words(slug)
                return candidate, domain or "basenotes", bool(candidate)

        if "parfumo." in low:
            match = re.search(r"/Perfumes/([^/?#]+)/([^/?#]+)", clean_url, re.I)
            if match:
                brand = QueryRepair._slug_to_words(match.group(1))
                name = QueryRepair._slug_to_words(match.group(2))
                candidate = QueryRepair.clean_suggestion(f"{brand} {name}")
                return candidate, domain or "parfumo", bool(candidate)

        return "", domain, False

    @staticmethod
    def _candidate_from_url(href: str) -> str:
        candidate, _, ok = QueryRepair._canonical_candidate_from_url(href)
        return candidate if ok else ""

    @staticmethod
    def _strip_title_tail(text: str) -> str:
        text = TextSanitizer.clean(text)
        text = QueryRepair.TITLE_TAIL_RE.sub("", text)
        text = re.sub(r"\b(?:19|20)\d{2}\b.*$", "", text)
        text = re.sub(r"\s+[-–—|]\s*(?:fragrantica|basenotes|parfumo)\b.*$", "", text, flags=re.I)
        return TextSanitizer.SPACE.sub(" ", text).strip(" -–—|/\t\n\r")

    @staticmethod
    def _title_candidates(text: str) -> list[str]:
        text = TextSanitizer.clean(text)

        if not text:
            return []

        text = re.sub(r"(?i)\s+\|\s+.*$", "", text)
        raw_pieces = [p.strip() for p in re.split(r"\s+[-–—|]\s+", text) if p and p.strip()]
        pieces = raw_pieces or [text]
        out: list[str] = []

        whole = QueryRepair._strip_title_tail(text)
        by_match = re.match(r"(?i)^(.+?)\s+by\s+(.+)$", whole)

        if by_match:
            name = QueryRepair.clean_suggestion(by_match.group(1))
            brand = QueryRepair.clean_suggestion(by_match.group(2))
            candidate = QueryRepair.clean_suggestion(f"{brand} {name}")
            if candidate:
                out.append(candidate)

        if len(pieces) >= 2:
            left = QueryRepair._strip_title_tail(pieces[0])
            right = QueryRepair._strip_title_tail(pieces[1])

            if left and right and QueryRepair._identity(right) not in {"fragrantica", "basenotes", "parfumo"}:
                combined = QueryRepair.clean_suggestion(f"{right} {left}")
                if combined:
                    out.append(combined)

        for piece in pieces[:3]:
            cleaned = QueryRepair.clean_suggestion(QueryRepair._strip_title_tail(piece))
            if cleaned:
                out.append(cleaned)

        seen = set()
        unique: list[str] = []

        for item in out:
            key = QueryRepair._identity(item)
            if key and key not in seen:
                seen.add(key)
                unique.append(item)

        return unique

    @staticmethod
    def _bad_candidate_text(text: str) -> bool:
        ident = QueryRepair._identity(text)

        if not ident or ident in QueryRepair.BAD_EXACT_SUGGESTIONS:
            return True

        words = ident.split()

        if any(word in QueryRepair.BAD_SUGGESTION_PARTS for word in words):
            return True

        # Block autocomplete/question rewrites.
        # Example:
        #   "dior sauvage reviews" -> "does dior sauvage smell good"
        # This is not a canonical fragrance query.
        if re.search(
            r"(?i)\b("
            r"does|do|did|is|are|was|were|can|should|would|could|"
            r"what|which|why|how|where|when"
            r")\b",
            text,
        ):
            return True

        if re.search(r"(?i)\b(smell|smells|smelling)\s+(good|bad|like)\b", text):
            return True

        if len(words) == 1 and words[0] in QueryRepair.GENERIC_TOKENS:
           return True

        return False

    @staticmethod
    def _make_record(text: str) -> dict:
        return {
            "text": text,
            "domains": set(),
            "canonical": 0,
            "title": 0,
            "direct": 0,
            "occurrences": 0,
            "manual_bonus": 0.0,
        }


    @staticmethod
    def _local_tail_cleanup(query: str) -> str:
        original = QueryRepair.normalized_query(query)
        cleaned = QueryRepair.clean_suggestion(original)

        if not cleaned:
            return ""

        if QueryRepair._identity(cleaned) == QueryRepair._identity(original):
            return ""

        original_tokens = QueryRepair._tokens(original)
        cleaned_tokens = QueryRepair._tokens(cleaned)

        # Do not collapse to brand-only.
        # Safe:
        #   "dior sauvage reviews" -> "dior sauvage"
        # Unsafe:
        #   "dior reviews" -> "dior"
        if len(cleaned_tokens) < 2:
           return ""

        # The cleaned query must preserve at least the first two meaningful tokens
        # when possible. This blocks bad drops like:
        #   "dior sauvage description" -> "sauvage description"
        if len(original_tokens) >= 2:
            preserved = 0

            for src in original_tokens[:2]:
                if any(QueryRepair._token_score(src, cand) >= 0.90 for cand in cleaned_tokens):
                    preserved += 1
  
            if preserved < 2:
                return ""

        return QueryRepair.normalized_query(cleaned)






    @staticmethod
    def _prefer_text(current: str, new: str, canonical: bool = False) -> str:
        if not current:
            return new

        current_alpha = sum(ch.isalpha() for ch in current)
        new_alpha = sum(ch.isalpha() for ch in new)

        if canonical and len(new.split()) <= QueryRepair.MAX_CANONICAL_WORDS:
            if new_alpha >= current_alpha and len(new) <= max(len(current) + 12, len(current) * 2):
                return new

        if current == current.lower() and new != new.lower():
            return new

        if 2 <= len(new.split()) <= len(current.split()) and len(new) <= len(current):
            return new

        return current

    @staticmethod
    def _add_candidate(
        records: dict,
        candidate: str,
        source: str = "",
        canonical: bool = False,
        title: bool = False,
        direct: bool = False,
        bonus: float = 0.0,
    ) -> None:
        candidate = QueryRepair.clean_suggestion(candidate)

        if not candidate or QueryRepair._bad_candidate_text(candidate):
            return

        words = QueryRepair._identity(candidate).split()
        max_words = QueryRepair.MAX_CANONICAL_WORDS if canonical else QueryRepair.MAX_SUGGESTION_WORDS

        if len(words) > max_words:
            return

        key = QueryRepair._identity(candidate)

        if not key:
            return

        rec = records.get(key)

        if rec is None:
            rec = QueryRepair._make_record(candidate)
            records[key] = rec
        else:
            rec["text"] = QueryRepair._prefer_text(str(rec.get("text", "")), candidate, canonical)

        rec["occurrences"] = int(rec.get("occurrences", 0)) + 1
        rec["manual_bonus"] = float(rec.get("manual_bonus", 0.0)) + float(bonus or 0.0)

        if source:
            source = source.lower().removeprefix("www.")
            if canonical and any(hint in source for hint in QueryRepair.CANONICAL_DOMAIN_HINTS):
                rec["domains"].add(source)
            elif direct:
                rec["domains"].add(source)

        if canonical:
            rec["canonical"] = int(rec.get("canonical", 0)) + 1

        if title:
            rec["title"] = int(rec.get("title", 0)) + 1

        if direct:
            rec["direct"] = int(rec.get("direct", 0)) + 1

    @staticmethod
    def _record_score(original: str, rec: dict) -> float:
        text = str(rec.get("text", ""))
        canonical_count = int(rec.get("canonical", 0))
        title_count = int(rec.get("title", 0))
        direct_count = int(rec.get("direct", 0))
        occurrences = int(rec.get("occurrences", 0))
        domain_count = len(rec.get("domains", set()) or set())

        if not text:
            return 0.0

        # Critical fix:
        # Direct/autocomplete/title evidence may not "repair" by only reordering the same tokens.
        # Example blocked: "Dior Sava" -> "sava dior"
        # Canonical URL evidence is allowed to reorder because URL structure proves real identity.
        if canonical_count <= 0 and QueryRepair._is_reorder_only(original, text):
            return 0.0

        if canonical_count <= 0 and not QueryRepair._adds_or_repairs_meaningful_token(original, text):
            return 0.0

        # Critical safety:
        # Non-canonical suggestions may not drop the first meaningful token.
        if canonical_count <= 0 and len(QueryRepair._tokens(original)) >= 3:
            if not QueryRepair._preserves_first_meaningful_token(original, text):
                return 0.0
            
        if canonical_count <= 0:
            if not QueryRepair._preserves_short_or_tail_token(original, text):
                return 0.0
        
        
        base = QueryRepair._candidate_score(original, text, allow_long=canonical_count > 0)

        try:
            base = float(base or 0.0)
        except Exception:
            return 0.0

        if base <= 0.0:
            return 0.0

        bonus = float(rec.get("manual_bonus", 0.0))

        if canonical_count:
            bonus += min(0.30, 0.20 + (0.04 * min(canonical_count - 1, 3)))

        if domain_count >= 2:
            bonus += 0.18
        elif domain_count == 1 and canonical_count:
            bonus += 0.07

        if occurrences >= 2:
            bonus += min(0.12, 0.04 * (occurrences - 1))

        if direct_count:
            bonus += min(0.12, 0.08 + (0.02 * min(direct_count - 1, 2)))

        if title_count and not canonical_count:
            bonus += min(0.04, 0.02 * title_count)

        original_tokens = QueryRepair._tokens(original)
        suggestion_tokens = QueryRepair._tokens(text)
        matches = QueryRepair._match_scores(original_tokens, suggestion_tokens)

        exact = sum(1 for src, cand, score in matches if src == cand and score >= 1.0)
        fuzzy = sum(1 for src, cand, score in matches if src != cand and score >= QueryRepair.MIN_DIRECT_SCORE)

        if exact:
            bonus += min(0.08, 0.03 * exact)

        if fuzzy:
            bonus += min(0.08, 0.03 * fuzzy)

        return base + bonus

    @staticmethod
    def _best_record(original: str, records: dict, minimum: float | None = None) -> str:
        minimum = QueryRepair.MIN_REPAIR_CONFIDENCE if minimum is None else minimum
        best_text = ""
        best_score = 0.0

        for rec in records.values():
            canonical_count = int(rec.get("canonical", 0))
            direct_count = int(rec.get("direct", 0))
            title_count = int(rec.get("title", 0))

            score = QueryRepair._record_score(original, rec)

            if score <= 0:
                continue

            required = minimum

            if title_count and not canonical_count and not direct_count:
                required = max(required, QueryRepair.MIN_TITLE_ONLY_CONFIDENCE)

            if score < required:
                continue

            text = QueryRepair.normalized_query(str(rec.get("text", "")))
            key = QueryRepair._identity(text)

            if not text or key == QueryRepair._identity(original):
                continue

            if score > best_score:
                best_score = score
                best_text = text

        return best_text

    @staticmethod
    def _best_candidate(original: str, candidates: list[tuple[str, float]]) -> str:
        records: dict = {}

        for candidate, bonus in candidates:
            QueryRepair._add_candidate(records, candidate, source="manual", direct=True, bonus=bonus)

        return QueryRepair._best_record(original, records, minimum=QueryRepair.MIN_DIRECT_SCORE)

    @staticmethod
    def _raw_urls_from_markup(markup: str) -> list[str]:
        text = html.unescape(markup or "").replace("\\/", "/")
        urls = re.findall(r"https?://[^\s\"'<>]+", text)
        clean: list[str] = []

        for url in urls:
            url = url.rstrip(").,;]")
            if url and url not in clean:
                clean.append(url)

        return clean[:80]

    @staticmethod
    def _direct_suggestion_patterns(kind: str) -> tuple[str, ...]:
        if kind == "bing":
            return (
                r"(?i)Including results for\s+(.+?)\s+Search only for",
                r"(?i)Did you mean:?\s+(.+?)(?:\s+Search only for|\s+All\b|\s+Images\b|\s+Videos\b|$)",
            )

        return (
            r"(?i)Showing results for\s+(.+?)\s+Search instead for",
            r"(?i)Did you mean:?\s+(.+?)(?:\s+Search instead for|\s+Showing results|\s+All\b|\s+Images\b|\s+Videos\b|$)",
            r"(?i)Showing results for\s+(.+?)(?:\s+Search instead for|\s+All\b|\s+Images\b|\s+Videos\b|$)",
        )

    @staticmethod
    def _add_markup_evidence(records: dict, markup: str, original: str, kind: str = "google") -> None:
        soup = BeautifulSoup(markup or "", "html.parser")
        page_text = soup.get_text(" ", strip=True)

        for pattern in QueryRepair._direct_suggestion_patterns(kind):
            match = re.search(pattern, page_text)
            if match:
                candidate = QueryRepair.clean_suggestion(match.group(1))
                if candidate:
                    QueryRepair._add_candidate(records, candidate, source=f"{kind}_suggest", direct=True)

        selectors = (".b_spellcheck a[href]", ".b_algo a[href]", "a[href]") if kind == "bing" else ("a[href]",)

        for selector in selectors:
            for a in soup.select(selector):
                label = a.get_text(" ", strip=True)
                href = html.unescape(a.get("href", ""))

                candidate, domain, is_canonical = QueryRepair._canonical_candidate_from_url(href)

                if is_canonical:
                    QueryRepair._add_candidate(records, candidate, source=domain, canonical=True)

                    for title_candidate in QueryRepair._title_candidates(label):
                        QueryRepair._add_candidate(records, title_candidate, source=domain, title=True)
                    continue

                url = QueryRepair._unwrap_result_url(href)
                domain = QueryRepair._domain_from_url(url)

                if url and not QueryRepair._blocked_url(url) and any(hint in domain for hint in QueryRepair.CANONICAL_DOMAIN_HINTS):
                    for title_candidate in QueryRepair._title_candidates(label):
                        QueryRepair._add_candidate(records, title_candidate, source=domain, title=True)

        for raw_url in QueryRepair._raw_urls_from_markup(markup):
            candidate, domain, is_canonical = QueryRepair._canonical_candidate_from_url(raw_url)
            if is_canonical:
                QueryRepair._add_candidate(records, candidate, source=domain, canonical=True)

    @staticmethod
    def _candidates_from_links(soup, original: str, selectors: tuple[str, ...]) -> str:
        records: dict = {}

        for selector in selectors:
            for a in soup.select(selector):
                label = a.get_text(" ", strip=True)
                href = html.unescape(a.get("href", ""))

                candidate, domain, is_canonical = QueryRepair._canonical_candidate_from_url(href)

                if is_canonical:
                    QueryRepair._add_candidate(records, candidate, source=domain, canonical=True)
                    for title_candidate in QueryRepair._title_candidates(label):
                        QueryRepair._add_candidate(records, title_candidate, source=domain, title=True)
                else:
                    url = QueryRepair._unwrap_result_url(href)
                    domain = QueryRepair._domain_from_url(url)
                    if url and not QueryRepair._blocked_url(url) and any(hint in domain for hint in QueryRepair.CANONICAL_DOMAIN_HINTS):
                        for title_candidate in QueryRepair._title_candidates(label):
                            QueryRepair._add_candidate(records, title_candidate, source=domain, title=True)

        return QueryRepair._best_record(original, records, minimum=QueryRepair.MIN_DIRECT_SCORE)

    @staticmethod
    def extract_google_suggestion(markup: str, original: str) -> str:
        records: dict = {}
        QueryRepair._add_markup_evidence(records, markup, original, kind="google")
        return QueryRepair._best_record(original, records, minimum=QueryRepair.MIN_DIRECT_SCORE)

    @staticmethod
    def extract_bing_suggestion(markup: str, original: str) -> str:
        records: dict = {}
        QueryRepair._add_markup_evidence(records, markup, original, kind="bing")
        return QueryRepair._best_record(original, records, minimum=QueryRepair.MIN_DIRECT_SCORE)

    @staticmethod
    def extract_google_autocomplete(markup: str, original: str) -> str:
        try:
            payload = json.loads(markup or "[]")
        except Exception:
            return ""

        suggestions = (
            payload[1]
            if isinstance(payload, list)
            and len(payload) > 1
            and isinstance(payload[1], list)
            else []
        )

        records: dict = {}

        for raw in suggestions:
            label = raw[0] if isinstance(raw, list) and raw else raw
            candidate = QueryRepair.clean_suggestion(str(label))
            if candidate:
                QueryRepair._add_candidate(records, candidate, source="google_autocomplete", direct=True)

        return QueryRepair._best_record(original, records, minimum=QueryRepair.MIN_DIRECT_SCORE)

    @staticmethod
    def _search_url(engine: str, query: str) -> str:
        from urllib.parse import quote

        if engine == "bing":
            return f"https://www.bing.com/search?q={quote(query)}"

        if engine == "autocomplete":
            return f"https://suggestqueries.google.com/complete/search?client=firefox&q={quote(query)}"

        return f"https://www.google.com/search?q={quote(query)}"

    @staticmethod
    def suggest(scraper, query: str, seconds: float = 1.6) -> str:

        query = QueryRepair.normalized_query(query)

        if not query:
           return ""
        
        question_cleanup = QueryRepair._question_intent_cleanup(query)

        if question_cleanup:
            return QueryRepair.normalized_query(question_cleanup)


        # First handle obvious metadata/page-tail cleanup locally.
        # Do this before Google/Bing/autocomplete so they cannot rewrite:
        #   "dior sauvage reviews" -> "does dior sauvage smell good"
        local_cleanup = QueryRepair._local_tail_cleanup(query)

        if local_cleanup:
            return QueryRepair.normalized_query(local_cleanup)

        deadline = Deadline(seconds)
        records: dict = {}

        # Google autocomplete (suggestqueries.google.com) returns compact JSON
        # and is stable across IPs. The SERP HTML scrape below is NOT: datacenter
        # hosts (e.g. Railway) get a JS-shell page with no result links, so the
        # scrape harvests nothing AND burns the whole deadline before the
        # autocomplete fallback ever runs. Seed evidence from autocomplete FIRST
        # so a dead SERP scrape can't starve it of the budget.
        autocomplete_res = Http.get(
            scraper,
            QueryRepair._search_url("autocomplete", f"{query} fragrance"),
            timeout=0.85,
            deadline=deadline,
            attempts=1,
        )
        if autocomplete_res:
            autocomplete_hint = QueryRepair.extract_google_autocomplete(
                autocomplete_res.text, query
            )
            if autocomplete_hint:
                QueryRepair._add_candidate(
                    records,
                    autocomplete_hint,
                    source="google_autocomplete",
                    direct=True,
                )

        tokens = QueryRepair._tokens(query)

        anchor_queries: list[str] = []

        if len(tokens) >= 2:
        # High-value adjacent anchors first.
        # These recover noisy tails:
        # "creed irish wata"  -> "creed irish"
        # "creed irish tower" -> "creed irish"
        # "irish tweed cree"  -> "irish tweed"
            for i in range(len(tokens) - 1):
                anchor_queries.append(" ".join(tokens[i:i + 2]))

        # Then useful edge pairs for reversed/partial inputs.
            anchor_queries.append(f"{tokens[0]} {tokens[-1]}")

        if len(tokens) >= 3:
        # Then drop-one forms, but after clean 2-token anchors.
            for i in range(len(tokens)):
                reduced = " ".join(tokens[:i] + tokens[i + 1:])
                if len(reduced.split()) >= 2:
                    anchor_queries.append(reduced)

        anchor_queries = list(dict.fromkeys(anchor_queries))[:5]

        canonical_queries = (
            *[f"{anchor} site:fragrantica.com/perfume" for anchor in anchor_queries],
            *[f"{anchor} site:basenotes.com/fragrances" for anchor in anchor_queries[:2]],
            *[f"{anchor} site:parfumo.com/Perfumes" for anchor in anchor_queries[:2]],
            f"{query} site:fragrantica.com/perfume",
            f"{query} fragrance",
            f"{query} perfume",
            f"{query} cologne",
        )

        search_plan: list[tuple[str, str]] = []

        for search_query in canonical_queries:
            search_plan.append(("google", search_query))

        if canonical_queries:
            search_plan.insert(1, ("bing", canonical_queries[0]))

        if len(canonical_queries) > 1:
            search_plan.insert(3, ("bing", canonical_queries[1]))

        search_plan = search_plan[:10]

        for search_query in canonical_queries:
            search_plan.append(("google", search_query))

        search_plan.insert(3, ("bing", canonical_queries[0]))

        if len(canonical_queries) > 3:
            search_plan.insert(4, ("bing", canonical_queries[3]))

        search_plan = search_plan[:10]
        
        for engine, search_query in search_plan:
            if deadline.expired():
                break

            res = Http.get(
                scraper,
                QueryRepair._search_url(engine, search_query),
                timeout=0.75,
                deadline=deadline,
                attempts=1,
            )

            if not res:
                continue

            QueryRepair._add_markup_evidence(records, res.text, query, kind=("bing" if engine == "bing" else "google"))

        best = QueryRepair._best_record(query, records)

        if best:
            return QueryRepair.normalized_query(best)

        fallback_plan: list[tuple[str, str]] = [
            ("autocomplete", f"{query} fragrance"),
            ("google", f"{query} perfume"),
            ("bing", f"{query} perfume"),
        ]

        for engine, search_query in fallback_plan:
            if deadline.expired():
                break

            res = Http.get(
                scraper,
                QueryRepair._search_url(engine, search_query),
                timeout=0.85,
                deadline=deadline,
                attempts=1,
            )

            if not res:
                continue

            if engine == "autocomplete":
                suggestion = QueryRepair.extract_google_autocomplete(res.text, query)
                if suggestion:
                    QueryRepair._add_candidate(records, suggestion, source="google_autocomplete", direct=True)
            else:
                QueryRepair._add_markup_evidence(records, res.text, query, kind=("bing" if engine == "bing" else "google"))

        best = QueryRepair._best_record(query, records)

        if best:
            return QueryRepair.normalized_query(best)

        anchor = QueryRepair._anchor_fallback_query(query)

        if anchor and QueryRepair._identity(anchor) != QueryRepair._identity(query):
            return QueryRepair.normalized_query(anchor)

        return ""

class Http:
    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    
    @staticmethod
    def get(
        scraper,
        url: str,
        timeout: float = 15,
        referer: str = "",
        deadline: Deadline | None = None,
        attempts: int = 1,
        sleep_seconds: float = 0.25,
    ):
        headers = dict(Http.DEFAULT_HEADERS)
        if referer:
            headers["Referer"] = referer
        last_error: Exception | None = None
        for attempt in range(max(1, attempts)):
            if deadline and deadline.expired():
                break
            try:
                request_timeout = deadline.timeout(timeout) if deadline else timeout
                res = scraper.get(url, timeout=request_timeout, headers=headers)
                res.raise_for_status()
                return res
            except Exception as exc:  # network parser must degrade cleanly
                last_error = exc
                if attempt >= attempts - 1:
                    break
                if deadline and deadline.remaining(0) is not None and deadline.remaining(0) <= sleep_seconds:
                    break
                time.sleep(sleep_seconds)
        return None

_BASENOTES_CACHE_FILE = Path(
    os.environ.get(
        "BASENOTES_CLEARANCE_CACHE",
        str(Path(__file__).with_name(".black_sun_cache.json")),
    )
)
_BASENOTES_CHALLENGE_MARKERS = (
    "just a moment",
    "attention required",
    "cf-challenge",
    "cf-browser-verification",
    "/cdn-cgi/challenge-platform",
)
_BASENOTES_SESSION_LOCK = threading.Lock()
_BASENOTES_SESSION = None


def _response_has_challenge(res: Any) -> bool:
    if res is None:
        return True
    status = int(getattr(res, "status_code", 0) or 0)
    body = str(getattr(res, "text", "") or "")
    if status in {403, 429}:
        return True
    return any(marker in body[:5000].lower() for marker in _BASENOTES_CHALLENGE_MARKERS)


def _new_basenotes_http_session(user_agent: str, cookies: dict[str, str]):
    if curl_requests is None:
        return None
    session = curl_requests.Session(
        impersonate=os.environ.get("BASENOTES_CURL_IMPERSONATE", "chrome120")
    )
    session.headers.update({
        "User-Agent": user_agent,
        "Accept": Http.DEFAULT_HEADERS["Accept"],
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": BasenotesEngine.BASE_URL if "BasenotesEngine" in globals() else "https://basenotes.com/",
    })
    session.cookies.update(cookies)
    return session


def _load_basenotes_cache() -> tuple[str, dict[str, str]] | None:
    try:
        with _BASENOTES_CACHE_FILE.open("r", encoding="utf-8") as handle:
            cache = json.load(handle)
        user_agent = str(cache.get("ua") or "").strip()
        cookies = cache.get("cookies") or {}
        if not user_agent or not isinstance(cookies, dict):
            return None
        return user_agent, {str(k): str(v) for k, v in cookies.items()}
    except Exception:
        return None


def _save_basenotes_cache(user_agent: str, cookies: dict[str, str]) -> None:
    try:
        _BASENOTES_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with _BASENOTES_CACHE_FILE.open("w", encoding="utf-8") as handle:
            json.dump({"ua": user_agent, "cookies": cookies}, handle)
    except Exception:
        pass


def _validate_basenotes_session(session: Any) -> bool:
    try:
        res = session.get("https://basenotes.com/", timeout=5)
        return int(getattr(res, "status_code", 0) or 0) == 200 and not _response_has_challenge(res)
    except Exception:
        return False


def _mint_basenotes_clearance():
    if curl_requests is None or ChromiumOptions is None or ChromiumPage is None:
        return None

    page = None
    try:
        options = ChromiumOptions()
        options.set_argument("--window-position=-2000,-2000")
        options.set_argument("--mute-audio")
        options.set_argument("--no-sandbox")
        options.set_argument("--disable-dev-shm-usage")
        if os.environ.get("BASENOTES_CHROMIUM_HEADLESS", "").lower() in {"1", "true", "yes"}:
            options.set_argument("--headless=new")
        options.auto_port()

        page = ChromiumPage(options)
        page.get("https://basenotes.com/")

        wait_seconds = int(os.environ.get("BASENOTES_CLEARANCE_WAIT", "20") or "20")
        for _ in range(max(1, wait_seconds)):
            title = str(getattr(page, "title", "") or "")
            if not any(marker in title.lower() for marker in ("just a moment", "attention required")):
                break
            time.sleep(1)

        cookie_data = page.cookies()
        cookies = (
            {str(c["name"]): str(c["value"]) for c in cookie_data if "name" in c and "value" in c}
            if isinstance(cookie_data, list)
            else {str(k): str(v) for k, v in dict(cookie_data or {}).items()}
        )
        user_agent = str(page.run_js("return navigator.userAgent;") or Http.DEFAULT_HEADERS["User-Agent"])
    except Exception:
        return None
    finally:
        if page is not None:
            try:
                page.quit()
            except Exception:
                pass

    if not cookies:
        return None
    _save_basenotes_cache(user_agent, cookies)
    session = _new_basenotes_http_session(user_agent, cookies)
    return session if session is not None and _validate_basenotes_session(session) else None


def get_basenotes_scraper(fallback: Any = None):
    global _BASENOTES_SESSION
    with _BASENOTES_SESSION_LOCK:
        if _BASENOTES_SESSION is not None:
            return _BASENOTES_SESSION

        cached = _load_basenotes_cache()
        if cached:
            session = _new_basenotes_http_session(*cached)
            if session is not None and _validate_basenotes_session(session):
                _BASENOTES_SESSION = session
                return _BASENOTES_SESSION

        minted = _mint_basenotes_clearance()
        if minted is not None:
            _BASENOTES_SESSION = minted
            return _BASENOTES_SESSION

    return fallback


def reset_basenotes_scraper(clear_cache: bool = False) -> None:
    global _BASENOTES_SESSION
    with _BASENOTES_SESSION_LOCK:
        _BASENOTES_SESSION = None
        if clear_cache:
            try:
                _BASENOTES_CACHE_FILE.unlink(missing_ok=True)
            except Exception:
                pass


class RoutedScraper:
    def __init__(self, default_scraper: Any, basenotes_scraper: Any = None):
        self.default_scraper = default_scraper
        self._basenotes_scraper = basenotes_scraper

    def _for_url(self, url: str):
        host = (urlparse(url).netloc or "").lower()
        if host.endswith("basenotes.com"):
            self._basenotes_scraper = get_basenotes_scraper(self._basenotes_scraper or self.default_scraper)
            return self._basenotes_scraper or self.default_scraper
        return self.default_scraper

    def get(self, url: str, *args, **kwargs):
        return self._for_url(url).get(url, *args, **kwargs)

    def __getattr__(self, name: str):
        return getattr(self.default_scraper, name)


def get_scraper():
    if cloudscraper is not None:
        default = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True}
        )
    else:
        default = requests.Session()
    return RoutedScraper(default, get_basenotes_scraper(default))


def draw_bar(pct: float | int | None, width: int = 20, color: str = G, empty_label: str = "No Data") -> str:
    if pct is None:
        return f"{D}[{empty_label}]{Z}".ljust(width)
    pct = max(0.0, min(float(pct), 100.0))
    filled = int(round((pct / 100.0) * width))
    return f"{color}{'█' * filled}{D}{'░' * (width - filled)}{Z}"

def sentiment_cell(item: UnifiedFragrance) -> str:
    if item.bn_positive_pct < 0:
        return "—"
    return f"{item.bn_positive_pct}%" + (f"/{item.bn_vote_count}" if item.bn_vote_count else "")

def link_cell(item: UnifiedFragrance) -> str:
    return "★" if item.frag_url else "—"

def match_cell(item: UnifiedFragrance) -> str:
    return f"{int(round(item.query_score * 100))}%"

def safe_get_text(node, separator: str = " ") -> str:
    if not node:
        return ""
    return TextSanitizer.clean(node.get_text(separator=separator, strip=True))

def unique_clean(values: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = TextSanitizer.clean(value)
        key = TextSanitizer.normalize_identity(text)
        if text and key and key not in seen:
            seen.add(key)
            out.append(text)
    return out

# --- Olfactory-note de-duplication -------------------------------------------
# Curated true-synonym table: genuinely different *names* for the same note.
# Each variant maps to a shared canonical key (all in normalize_identity form).
# Deliberate and always safe to apply — extend freely.
NOTE_SYNONYMS = {
    # wood notes
    "cedarwood": "cedar", "cedar wood": "cedar",
    "agarwood": "oud", "agar wood": "oud", "oudh": "oud", "aoud": "oud",
    "oud wood": "oud",
    "guaiacwood": "guaiac wood", "gaiac wood": "guaiac wood",
    "gaiacwood": "guaiac wood", "guaiac": "guaiac wood",
    "santal": "sandalwood",
    # resins / balsams
    "olibanum": "frankincense",
    # spelling variants
    "patchouly": "patchouli",
    "vetivert": "vetiver",
    "mandarine": "mandarin",
    "ylangylang": "ylang ylang", "ylang": "ylang ylang",
    # aroma-chemicals (trade-name / spacing variants)
    "ambrox": "ambroxan", "ambroxide": "ambroxan",
    "iso e": "iso e super", "isoe super": "iso e super",
    # botanical equivalents
    "muguet": "lily of the valley",
    "helichrysum": "immortelle", "everlasting flower": "immortelle",
    "orris root": "orris", "iris root": "iris",
    # "X bean / seed" forms
    "tonka bean": "tonka", "tonka beans": "tonka",
    "vanilla bean": "vanilla", "vanilla beans": "vanilla",
    "ambrette seed": "ambrette", "ambrette seeds": "ambrette",
}

# Words that qualify a note without changing what it *is* — origin/provenance
# ("madagascar vanilla"), form/preparation ("rose absolute"), or structural
# filler ("leather accord"). Peeled off the ends of an identity. Deliberately
# conservative: varietal-defining words (sichuan, pink, black, smoked, ...) are
# NOT here, so genuinely distinct notes are never merged. Safe to extend.
NOTE_QUALIFIER_WORDS = frozenset({
    # origin / provenance
    "madagascar", "madagascan", "papua", "guinea", "guinean", "new",
    "indonesian", "indonesia", "tahitian", "tahiti", "sicilian", "sicily",
    "calabrian", "calabria", "italian", "egyptian", "bulgarian", "turkish",
    "moroccan", "morocco", "haitian", "brazilian", "australian", "russian",
    "somalian", "somali", "ethiopian", "virginian", "virginia", "mysore",
    "ceylon", "javanese", "java", "bourbon", "reunion", "florida",
    "indian", "india", "chinese", "china", "japanese", "japan",
    "vietnamese", "vietnam", "cambodian", "cambodia", "thai", "thailand",
    "sri", "lankan", "lanka", "burmese", "spanish", "spain", "french",
    "greek", "portuguese", "tunisian", "algerian", "lebanese", "syrian",
    "iranian", "persian", "arabian", "corsican", "corsica", "mexican",
    "peruvian", "guatemalan", "kenyan", "comorian", "comoros", "balkan",
    # form / preparation
    "absolute", "essence", "extract", "oil", "co2", "infusion",
    "tincture", "resinoid", "resin", "concrete", "isolate",
    # structural filler
    "accord", "note", "notes", "natural", "synthetic", "type", "scent",
})


def _strip_qualifiers(identity: str) -> str:
    """`identity` with leading/trailing qualifier words peeled off."""
    tokens = identity.split()
    while tokens and tokens[0] in NOTE_QUALIFIER_WORDS:
        tokens.pop(0)
    while tokens and tokens[-1] in NOTE_QUALIFIER_WORDS:
        tokens.pop()
    return " ".join(tokens)


# Junk characters that should never appear in a displayed olfactory note:
# periods and trademark / registered / copyright / service-mark / sound-recording
# symbols, plus common bullet/section glyphs that occasionally leak from scraped
# HTML. Stripped from display only — identity keys already drop them.
_NOTE_DISPLAY_JUNK_RE = re.compile(
    r"[.™®©℠℗•·¶§⁃◦]"
)


def _clean_note_display(text: str) -> str:
    text = _NOTE_DISPLAY_JUNK_RE.sub("", text)
    text = TextSanitizer.SPACE.sub(" ", text).strip(" -–—,;:")
    return text


def dedupe_notes(*layers: Iterable[str]) -> list[list[str]]:
    """De-duplicate olfactory notes within and across pyramid layers.

    One consistent pass over every layer. The unifying idea is
    *co-occurrence-gated canonicalization*: a lossy transform may only fold a
    note onto a simpler form when that simpler form actually appears in the
    data — so nothing is ever wrongly merged in isolation. Rules:

      1. case / spacing / diacritic variants always collapse (normalize_identity);
      2. curated true synonyms always collapse ("cedarwood" -> "cedar");
      3. qualifier variants ("madagascar vanilla", "leather accord") collapse
         onto their core ("vanilla", "leather") — but only when that core also
         appears as its own note, or two+ qualified variants share it;
      4. plurals collapse onto the singular ("roses" -> "rose") — but only when
         the singular also appears, so "iris"/"citrus" are never touched;
      5. a note kept in an earlier layer is dropped from later layers.

    The bare canonical form is displayed when present; otherwise the shortest
    variant wins. Returns one cleaned list per input layer, in order.
    """
    materialised = [list(layer) for layer in layers]

    def base_identity(note: str) -> str:
        ident = TextSanitizer.normalize_identity(note)
        return NOTE_SYNONYMS.get(ident, ident)

    # Pass 1: every identity present (synonym-folded), and which identities
    # strip down to each qualifier-free core.
    present: set[str] = set()
    core_to_idents: dict[str, set[str]] = {}
    for layer in materialised:
        for note in layer:
            ident = base_identity(note)
            if not ident:
                continue
            present.add(ident)
            core = NOTE_SYNONYMS.get(_strip_qualifiers(ident), _strip_qualifiers(ident))
            if core and core != ident:
                core_to_idents.setdefault(core, set()).add(ident)
    # A core is a valid merge target if it appears as a note in its own right,
    # or if two or more distinct qualified variants reduce to it.
    mergeable_cores = present | {c for c, ids in core_to_idents.items() if len(ids) >= 2}

    def canonical_key(note: str) -> str:
        ident = base_identity(note)
        if not ident:
            return ""
        core = NOTE_SYNONYMS.get(_strip_qualifiers(ident), _strip_qualifiers(ident))
        if core and core != ident and core in mergeable_cores:
            return core
        if len(ident) > 4 and ident.endswith("s") and ident[:-1] in present:
            return NOTE_SYNONYMS.get(ident[:-1], ident[:-1])
        return ident

    # Pass 2: choose the display text per key — the bare canonical form if it
    # appears, else the shortest variant.
    display: dict[str, str] = {}
    for layer in materialised:
        for note in layer:
            text = _clean_note_display(TextSanitizer.clean(note))
            key = canonical_key(note)
            if not text or not key:
                continue
            current = display.get(key)
            if current is None:
                display[key] = text
                continue
            # "bare" = the text is *literally* the canonical key, so the
            # canonical spelling wins display over any synonym/variant.
            cur_bare = TextSanitizer.normalize_identity(current) == key
            new_bare = TextSanitizer.normalize_identity(note) == key
            if (new_bare, -len(text)) > (cur_bare, -len(current)):
                display[key] = text

    # Pass 3: emit, deduping within and across layers by canonical key.
    seen: set[str] = set()
    result: list[list[str]] = []
    for layer in materialised:
        cleaned: list[str] = []
        for note in layer:
            key = canonical_key(note)
            if not key or key in seen:
                continue
            seen.add(key)
            cleaned.append(display[key])
        result.append(cleaned)
    return result

def normalize_notes(notes: NotesList) -> NotesList:
    notes.top, notes.heart, notes.base = dedupe_notes(notes.top, notes.heart, notes.base)
    notes.flat = dedupe_notes(notes.flat)[0]
    levels = [notes.top, notes.heart, notes.base]
    keys = [{TextSanitizer.normalize_identity(x) for x in level} for level in levels]
    if notes.has_pyramid and all(levels) and keys[0] == keys[1] == keys[2]:
        notes.has_pyramid = False
        notes.flat = dedupe_notes(notes.top)[0]
        notes.top = notes.heart = notes.base = []
    elif notes.has_pyramid and sum(bool(level) for level in levels) < 2:
        notes.has_pyramid = False
        notes.flat = dedupe_notes([*notes.top, *notes.heart, *notes.base, *notes.flat])[0]
        notes.top = notes.heart = notes.base = []
    elif not notes.has_pyramid:
        notes.flat = dedupe_notes([*notes.flat, *notes.top, *notes.heart, *notes.base])[0]
        notes.top = notes.heart = notes.base = []
    return notes

def review_key(text: str, width: int = 420) -> str:
    value = TextSanitizer.normalize_identity(text)
    value = re.sub(r"\b(show all reviews|reply|like|dislike|report)\b", " ", value)
    return TextSanitizer.SPACE.sub(" ", value).strip()[:width]

def dedupe_reviews(reviews: list[Review]) -> list[Review]:
    out: list[Review] = []
    seen: set[str] = set()
    for review in reviews:
        text = TextSanitizer.clean(review.text)
        key = review_key(text)
        if len(text) < 30 or not key or key in seen:
            continue
        seen.add(key)
        out.append(Review(text=text, source=review.source))
    return out

def metric_count(metric: dict[str, Any]) -> str:
    count = TextSanitizer.clean(metric.get("count", ""))
    return "" if count in {"", "0", "0 votes"} else count

def print_metric(metric: dict[str, Any], card_name: str = "") -> None:
    # Prefer a presentation override (e.g. Price Value '$' symbols) when set,
    # otherwise fall back to the canonical label.
    raw_label = metric.get("display") or metric.get("label", "Metric")
    label = TextSanitizer.clean(raw_label)[:22].ljust(22)
    value = TextSanitizer.clean(metric.get("value", ""))
    count = metric_count(metric)
    pct_raw = metric.get("pct", None)
    pct = float(pct_raw or 0) if isinstance(pct_raw, (int, float, str)) and str(pct_raw).strip() else 0.0
    if value:
        tail = f" {D}({count}){Z}" if count else ""
        print(f"   {label} {M}{value}{Z}{tail}")
    elif pct > 0:
        tail = f" {D}({count}){Z}" if count else ""
        print(f"   {label} {draw_bar(pct, 15, M if 'rating' in card_name.lower() else C)} {pct:>5.1f}%{tail}")
    elif count:
        print(f"   {label} {D}{count}{Z}")

def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value).replace("%", "").replace(",", "").strip())
    except Exception:
        return default

def _pct_from_metric(metric: dict[str, Any]) -> float:
    return max(0.0, min(100.0, _num(metric.get("pct"), 0.0)))

def _count_int(text: object) -> int:
    value = TextSanitizer.clean(text).lower().replace(",", "")
    m = re.search(r"([\d.]+)\s*([km]?)", value)
    if not m:
        return 0
    num = float(m.group(1))
    mult = 1000 if m.group(2) == "k" else 1000000 if m.group(2) == "m" else 1
    return int(num * mult)

def _dominant_metric(metrics: list[dict[str, Any]]) -> dict[str, Any] | None:
    valid = [m for m in metrics if TextSanitizer.clean(m.get("label", ""))]
    if not valid:
        return None
    return max(valid, key=lambda m: (_count_int(m.get("count", "")), _pct_from_metric(m)))

def _card_lookup(details: UnifiedDetails, *needles: str) -> tuple[str, list[dict[str, Any]]]:
    needles_l = [n.lower() for n in needles]
    for name, metrics in details.frag_cards.items():
        low = name.lower()
        if any(n in low for n in needles_l):
            return name, metrics
    return "", []

def _weighted_label_score(label: str, card_name: str) -> float | None:
    low = TextSanitizer.normalize_identity(f"{card_name} {label}")
    ordered = [
        (r"eternal|very long|long lasting|long", 92),
        (r"moderate", 62),
        (r"weak|soft", 36),
        (r"very weak", 18),
        (r"enormous|room filling", 96),
        (r"strong|heavy", 82),
        (r"intimate|close", 35),
        (r"excellent|great|amazing", 94),
        (r"good|high", 76),
        (r"okay|ok|average|fair", 55),
        (r"poor|bad|low|overpriced", 28),
        (r"like it|love it|really like", 84),
        (r"dislike|hate", 18),
    ]
    if "very weak" in low:
        return 18
    for pattern, score in ordered:
        if re.search(pattern, low):
            return float(score)
    return None

def _distribution_score(card_name: str, metrics: list[dict[str, Any]]) -> tuple[float | None, str]:
    weighted = []
    for metric in metrics:
        label = TextSanitizer.clean(metric.get("label", ""))
        pct = _pct_from_metric(metric)
        score = _weighted_label_score(label, card_name)
        if score is not None and pct > 0:
            weighted.append((score, pct, label))
    if weighted:
        total = sum(p for _, p, _ in weighted) or 1.0
        score = sum(s * p for s, p, _ in weighted) / total
        dominant = max(weighted, key=lambda x: x[1])[2]
        return max(0.0, min(100.0, score)), dominant
    dom = _dominant_metric(metrics)
    return None, TextSanitizer.clean(dom.get("label", "")) if dom else ""

def _fg_rating_score(details: UnifiedDetails) -> tuple[float | None, str, str]:
    _, metrics = _card_lookup(details, "fragrantica rating")
    for metric in metrics:
        value = TextSanitizer.clean(metric.get("value", ""))
        m = re.search(r"([0-5](?:\.\d+)?)\s*/\s*5", value)
        if m:
            rating = float(m.group(1))
            return rating / 5.0 * 100.0, value, metric_count(metric)
    return None, "", ""

def _bn_approval_score(details: UnifiedDetails) -> tuple[float | None, str]:
    if not details.bn_consensus:
        return None, ""
    pos_votes, pos_pct = details.bn_consensus.get("pos", (0, 0))
    neu_votes, neu_pct = details.bn_consensus.get("neu", (0, 0))
    neg_votes, neg_pct = details.bn_consensus.get("neg", (0, 0))
    total = pos_votes + neu_votes + neg_votes
    score = pos_pct + (neu_pct * 0.35)
    label = f"{pos_pct}% positive"
    if total:
        label += f" · {total} votes"
    return max(0.0, min(100.0, score)), label

def build_metric_scorecard(details: UnifiedDetails) -> dict[str, Any]:
    fg_score, fg_value, fg_count = _fg_rating_score(details)
    bn_score, bn_label = _bn_approval_score(details)
    lon_name, lon_metrics = _card_lookup(details, "longevity")
    sil_name, sil_metrics = _card_lookup(details, "sillage", "projection")
    val_name, val_metrics = _card_lookup(details, "price value", "value")
    gen_name, gen_metrics = _card_lookup(details, "gender")
    wear_name, wear_metrics = _card_lookup(details, "when to wear", "season", "wear")
    lon_score, lon_label = _distribution_score(lon_name, lon_metrics) if lon_metrics else (None, "")
    sil_score, sil_label = _distribution_score(sil_name, sil_metrics) if sil_metrics else (None, "")
    val_score, val_label = _distribution_score(val_name, val_metrics) if val_metrics else (None, "")
    perf_scores = [s for s in (lon_score, sil_score) if s is not None]
    perf_score = sum(perf_scores) / len(perf_scores) if perf_scores else None
    score_parts = [(fg_score, 0.45), (bn_score, 0.25), (perf_score, 0.20), (val_score, 0.10)]
    available = [(s, w) for s, w in score_parts if s is not None]
    overall = None if not available else sum(s * w for s, w in available) / sum(w for _, w in available)
    gender = TextSanitizer.clean((_dominant_metric(gen_metrics) or {}).get("label", "")) if gen_metrics else ""
    wear = TextSanitizer.clean((_dominant_metric(wear_metrics) or {}).get("label", "")) if wear_metrics else ""
    return {
        "overall": overall,
        "fg_rating": (fg_score, fg_value, fg_count),
        "bn_approval": (bn_score, bn_label),
        "performance": (perf_score, ", ".join(x for x in [lon_label and f"Longevity: {lon_label}", sil_label and f"Sillage: {sil_label}"] if x)),
        "value": (val_score, val_label),
        "gender_vote": gender,
        "wear_vote": wear,
        "source_count": len(available),
    }

def score_cell(score: float | None) -> str:
    return "—" if score is None else f"{int(round(score)):>3}/100"

def print_scorecard(details: UnifiedDetails) -> None:
    card = build_metric_scorecard(details)
    if card["overall"] is None:
        return
    print(f" {B}✦ OVERALL SCORECARD ✦{Z}")
    print(f"  Composite:   {score_cell(card['overall'])} {D}(normalized from available metrics; not a lab score){Z}")
    fg_score, fg_value, fg_count = card["fg_rating"]
    bn_score, bn_label = card["bn_approval"]
    perf_score, perf_label = card["performance"]
    val_score, val_label = card["value"]
    rows = [
        ("Rating", score_cell(fg_score), f"{fg_value} {fg_count}".strip()),
        ("Approval", score_cell(bn_score), bn_label),
        ("Performance", score_cell(perf_score), perf_label),
        ("Value", score_cell(val_score), val_label),
    ]
    for label, score, info in rows:
        if score != "—" or info:
            print(f"  {label:<12} {score:<7} {D}{info}{Z}")
    if card["gender_vote"] or card["wear_vote"]:
        print(f"  Profile      {D}{' · '.join(x for x in [card['gender_vote'], card['wear_vote']] if x)}{Z}")
    print()

def metric_group_key(card_name: str) -> tuple[int, str]:
    low = card_name.lower()
    order = [
        ("fragrantica rating", 0),
        ("main accords", 1),
        ("longevity", 2),
        ("sillage", 3),
        ("projection", 3),
        ("gender", 4),
        ("price value", 5),
        ("value", 5),
        ("when to wear", 6),
        ("season", 6),
    ]
    for needle, idx in order:
        if needle in low:
            return idx, card_name
    return 20, card_name

def print_fragrantica_metric_groups(details: UnifiedDetails) -> None:
    if not details.frag_cards:
        return
    print(f" {B}✦ METRICS BY CATEGORY ✦{Z}")
    for card_name, metrics in sorted(details.frag_cards.items(), key=lambda kv: metric_group_key(kv[0])):
        if not metrics:
            continue
        # Internal card keys may carry a source name; strip it from the display only.
        display_name = re.sub(r"(?i)\bfragrantica\s*", "", card_name).strip() or card_name
        print(f"  {Y}[{display_name}]{Z}")
        shown = 0
        for metric in metrics:
            print_metric(metric, card_name)
            shown += 1
        if shown:
            print()

def print_reviews_by_source(reviews: list[Review], limit_per_source: int = 4) -> None:
    # Reviews are shown without their origin label. Still draw evenly from each
    # internal source so the sample stays balanced.
    selected: list[Review] = []
    for source in ("Basenotes", "Fragrantica"):
        selected.extend([r for r in reviews if r.source.lower() == source.lower()][:limit_per_source])
    if not selected:
        return
    print(f"\n{B}✦ SAMPLE REVIEWS ✦{Z}\n{D}{'─' * 74}{Z}")
    for idx, review in enumerate(selected, 1):
        print(f"\n{Y}[{idx}]{Z} {review.text}\n{D}{'─' * 74}{Z}")

def bounded_parallel(
    jobs: dict[str, Callable[[], Any]],
    seconds: float,
    max_workers: int | None = None,
) -> dict[str, Any]:
    """Run independent jobs under one shared wall-clock deadline."""
    results: dict[str, Any] = {}
    executor = ThreadPoolExecutor(max_workers=max_workers or max(1, len(jobs)))
    future_to_name = {executor.submit(fn): name for name, fn in jobs.items()}
    deadline = time.monotonic() + max(0.0, seconds)
    pending = set(future_to_name)
    while pending:
        remaining = max(0.0, deadline - time.monotonic())
        if remaining <= 0:
            break
        done, pending = wait(pending, timeout=remaining, return_when=FIRST_COMPLETED)
        for future in done:
            name = future_to_name[future]
            try:
                results[name] = future.result(timeout=0)
            except Exception:
                results[name] = None
    for future in pending:
        future.cancel()
        results.setdefault(future_to_name[future], None)
    executor.shutdown(wait=False, cancel_futures=True)
    return results

def run_budgeted_items(
    items: Iterable[Any],
    worker: Callable[[Any], Any],
    seconds: float,
    max_workers: int = 5,
) -> None:
    item_list = list(items)
    if not item_list or seconds <= 0:
        return
    executor = ThreadPoolExecutor(max_workers=max(1, max_workers))
    futures = {executor.submit(worker, item): item for item in item_list}
    end = time.monotonic() + seconds
    pending = set(futures)
    while pending:
        remaining = max(0.0, end - time.monotonic())
        if remaining <= 0:
            break
        done, pending = wait(pending, timeout=remaining, return_when=FIRST_COMPLETED)
        for future in done:
            try:
                future.result(timeout=0)
            except Exception:
                pass
    for future in pending:
        future.cancel()
    executor.shutdown(wait=False, cancel_futures=True)

class IdentityCache:
    def __init__(self, path: str | Path | None):
        # Uses v2 cache to automatically bust any poisoned identity matches
        # from older looser matching algorithms (e.g. 1953 Eau de Cologne vs 2024 Paddock)
        default_path = Path.home() / ".cache" / "fragrance_parser_fg_identity_cache_v2.json"
        self.path = Path(path or default_path)
        self.data: dict[str, dict[str, Any]] = {}
        self.dirty = False
        self.load()
        
    def load(self) -> None:
        try:
            if self.path.exists():
                self.data = json.loads(self.path.read_text(encoding="utf-8"))
                if not isinstance(self.data, dict):
                    self.data = {}
        except Exception:
            self.data = {}
            
    def get(self, brand: str, name: str, year: str = "") -> str:
        keys = [IdentityTools.cache_key(brand, name, year)]
        if year:
            keys.append(IdentityTools.cache_key(brand, name, ""))
        for key in keys:
            row = self.data.get(key)
            if not isinstance(row, dict):
                continue
            url = str(row.get("url") or "")
            if FragranticaEngine.is_perfume_url(url):
                return url
        return ""
        
    def put(self, frag: UnifiedFragrance, source: str = "") -> None:
        if not frag.frag_url or not FragranticaEngine.is_perfume_url(frag.frag_url):
            return
        if frag.resolver_score and frag.resolver_score < Orchestrator.NATIVE_ACCEPT:
            return
        key = frag.cache_key
        if not key or key == "||":
            return
        self.data[key] = {
            "brand": TextSanitizer.clean(frag.brand),
            "name": TextSanitizer.clean(frag.name),
            "year": TextSanitizer.clean(frag.year),
            "url": frag.frag_url,
            "source": source or frag.resolver_source or "unknown",
            "updated_at": int(time.time()),
        }
        yearless = IdentityTools.cache_key(frag.brand, frag.name, "")
        self.data.setdefault(yearless, dict(self.data[key]))
        self.dirty = True
        
    def save(self) -> None:
        if not self.dirty:
            return
        tmp_path = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp_path_str = tempfile.mkstemp(prefix=self.path.name, suffix=".tmp", dir=str(self.path.parent))
            tmp_path = Path(tmp_path_str)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(self.data, handle, ensure_ascii=False, indent=2, sort_keys=True)
            os.replace(tmp_path, self.path)
            self.dirty = False
        except Exception:
            if tmp_path and tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass

class SearchSniper:
    _designer_catalog_cache: dict[str, list[CatalogItem]] = {}
    
    @staticmethod
    def apply_cache(candidates: list[UnifiedFragrance], cache: IdentityCache) -> int:
        linked = 0
        for item in candidates:
            if item.frag_url:
                continue
            url = cache.get(item.brand, item.name, item.year)
            if url:
                item.frag_url = url
                item.resolver_source = "fg_cache"
                item.resolver_score = 0.98
                linked += 1
        return linked
        
    @staticmethod
    def cache_successes(candidates: list[UnifiedFragrance], cache: IdentityCache, source: str = "") -> None:
        for item in candidates:
            if item.frag_url:
                cache.put(item, source=source or item.resolver_source)
        cache.save()
        
    @staticmethod
    def designer_slug_candidates(brand: str, limit: int = 6) -> list[str]:
        forms = list(IdentityTools.brand_forms(brand)) or [brand]
        slugs: list[str] = []
        for form in forms:
            cleaned = TextSanitizer.clean(form)
            if not cleaned:
                continue
            title_slug = re.sub(r"\s+", "-", cleaned.strip()).title()
            plain_slug = re.sub(r"\s+", "-", cleaned.strip())
            no_punct = re.sub(r"[^A-Za-z0-9\s-]", "", cleaned)
            no_punct_slug = re.sub(r"\s+", "-", no_punct.strip()).title()
            for slug in (title_slug, plain_slug, no_punct_slug):
                slug = slug.strip("-")
                if slug and slug not in slugs:
                    slugs.append(slug)
        return slugs[: max(1, limit)]
        
    @staticmethod
    def catalog_candidates_for_brand(
        scraper,
        brand: str,
        deadline: Deadline,
        slug_limit: int = 6,
        page_timeout: float = 3.5,
        trace: ResolverTrace | None = None,
    ) -> list[CatalogItem]:
        brand_key = TextSanitizer.normalize_identity(brand)
        if brand_key in SearchSniper._designer_catalog_cache:
            cached = SearchSniper._designer_catalog_cache[brand_key]
            if trace:
                token = trace.start("designer_catalog", brand=brand, deadline=deadline)
                trace.finish(token, result_count=len(cached), skipped_reason="catalog_cache_hit", deadline=deadline)
            return cached
        slug_candidates = SearchSniper.designer_slug_candidates(brand, limit=slug_limit)
        if not slug_candidates:
            if trace:
                trace.add_skip("designer_catalog", "no_designer_slug_candidates", brand=brand, deadline=deadline)
            return []
        catalog: list[CatalogItem] = []
        seen: set[str] = set()
        token = trace.start("designer_catalog", brand=brand, deadline=deadline) if trace else None
        for slug in slug_candidates:
            if deadline.expired():
                break
            url = f"{FragranticaEngine.BASE_URL}/designers/{quote(slug)}.html"
            res = Http.get(
                scraper,
                url,
                timeout=page_timeout,
                referer=FragranticaEngine.BASE_URL,
                deadline=deadline,
                attempts=1,
            )
            if not res:
                continue
            soup = BeautifulSoup(res.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = FragranticaEngine.canonical_url(a.get("href", ""))
                if not FragranticaEngine.is_perfume_url(href):
                    continue
                key = FragranticaEngine.dedupe_key(href)
                if key in seen:
                    continue
                seen.add(key)
                name = FragranticaEngine.name_from_url(href)
                parsed_brand = FragranticaEngine.brand_from_url(href) or brand
                if parsed_brand and not IdentityTools.compatible_brand(brand, parsed_brand):
                    continue
                label_text = safe_get_text(a)
                year_match = TextSanitizer.YEAR_RE.search(label_text)
                catalog.append(
                    CatalogItem(
                        name=name,
                        brand=parsed_brand,
                        year=year_match.group(0) if year_match else "",
                        url=href,
                    )
                )
            if catalog:
                break
        if catalog:
            SearchSniper._designer_catalog_cache[brand_key] = catalog
        if trace and token:
            trace.finish(token, result_count=len(catalog), skipped_reason="" if catalog else "no_perfume_links_found", deadline=deadline)
        return catalog
        
    @staticmethod
    def attach_from_designer_catalog(
        scraper,
        candidates: list[UnifiedFragrance],
        cache: IdentityCache,
        budget_seconds: float = 5.0,
        max_workers: int = 3,
        slug_limit: int = 6,
        trace: ResolverTrace | None = None,
    ) -> int:
        missing = [item for item in candidates if not item.frag_url and item.brand and item.name]
        if not missing or budget_seconds <= 0:
            if trace:
                trace.add_skip("designer_catalog", "already_linked" if not missing else "budget_exhausted")
            return 0
        deadline = Deadline(budget_seconds)
        branch_token = trace.start("designer_catalog", query=f"{len(missing)} missing", deadline=deadline) if trace else None
        brands = []
        for item in missing:
            key = TextSanitizer.normalize_identity(item.brand)
            if key and key not in brands:
                brands.append(key)
        catalogs_by_brand: dict[str, list[CatalogItem]] = {}
        def fetch_brand(brand_key: str) -> None:
            representative = next((m.brand for m in missing if TextSanitizer.normalize_identity(m.brand) == brand_key), brand_key)
            catalogs_by_brand[brand_key] = SearchSniper.catalog_candidates_for_brand(
                scraper,
                representative,
                deadline=deadline,
                slug_limit=slug_limit,
                trace=trace,
            )
        run_budgeted_items(brands, fetch_brand, seconds=budget_seconds, max_workers=max_workers)
        linked = 0
        for item in missing:
            brand_key = TextSanitizer.normalize_identity(item.brand)
            best: CatalogItem | None = None
            best_score = 0.0
            for catalog_item in catalogs_by_brand.get(brand_key, []):
                score = Orchestrator.identity_score(item, catalog_item)
                if score > best_score:
                    best = catalog_item
                    best_score = score
            if best and best_score >= Orchestrator.CATALOG_ACCEPT:
                item.frag_url = best.url
                item.resolver_source = "designer_catalog"
                item.resolver_score = best_score
                if not item.year and best.year:
                    item.year = best.year
                linked += 1
                if trace:
                    token = trace.start("designer_catalog", brand=item.brand, name=item.name, year=item.year, deadline=deadline)
                    trace.finish(token, linked_count=1, accepted_url=best.url, deadline=deadline)
                cache.put(item, source="designer_catalog")
            elif trace and best:
                trace.add_reject("designer_catalog", best.url, "score_below_threshold", brand=item.brand, name=item.name, year=item.year, deadline=deadline)
        cache.save()
        if trace and branch_token:
            trace.finish(branch_token, result_count=sum(len(v) for v in catalogs_by_brand.values()), linked_count=linked, skipped_reason="" if linked else "score_below_threshold", deadline=deadline)
        return linked
        
    @staticmethod
    def attach_related_from_known_pages(
        scraper,
        candidates: list[UnifiedFragrance],
        cache: IdentityCache,
        budget_seconds: float = 3.0,
        page_timeout: float = 2.5,
        max_pages: int = 3,
        trace: ResolverTrace | None = None,
    ) -> int:
        missing = [item for item in candidates if not item.frag_url]
        seeds = [item.frag_url for item in candidates if item.frag_url]
        seeds = list(dict.fromkeys(seeds))[:max_pages]
        if not missing or not seeds or budget_seconds <= 0:
            if trace:
                reason = "already_linked" if not missing else ("no_seed_urls" if not seeds else "budget_exhausted")
                trace.add_skip("related_page_crawl", reason)
            return 0
        deadline = Deadline(budget_seconds)
        branch_token = trace.start("related_page_crawl", query=f"{len(seeds)} seeds", deadline=deadline) if trace else None
        discovered: list[CatalogItem] = []
        seen: set[str] = set()
        def fetch_seed(seed_url: str) -> None:
            if deadline.expired():
                if trace:
                    trace.add_skip("related_page_crawl", "budget_exhausted", deadline=deadline)
                return
            res = Http.get(
                scraper,
                seed_url,
                timeout=page_timeout,
                referer=FragranticaEngine.BASE_URL,
                deadline=deadline,
                attempts=1,
            )
            if not res:
                return
            soup = BeautifulSoup(res.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = FragranticaEngine.canonical_url(a.get("href", ""))
                if not FragranticaEngine.is_perfume_url(href):
                    continue
                key = FragranticaEngine.dedupe_key(href)
                if key in seen:
                    continue
                seen.add(key)
                discovered.append(
                    CatalogItem(
                        name=FragranticaEngine.name_from_url(href),
                        brand=FragranticaEngine.brand_from_url(href),
                        year="",
                        url=href,
                    )
                )
        run_budgeted_items(seeds, fetch_seed, seconds=budget_seconds, max_workers=min(3, len(seeds)))
        linked = 0
        for item in missing:
            best: CatalogItem | None = None
            best_score = 0.0
            for candidate in discovered:
                if item.brand and candidate.brand and not IdentityTools.compatible_brand(item.brand, candidate.brand):
                    continue
                score = Orchestrator.identity_score(item, candidate)
                if score > best_score:
                    best = candidate
                    best_score = score
            if best and best_score >= Orchestrator.RELATED_ACCEPT:
                item.frag_url = best.url
                item.resolver_source = "related_known_pages"
                item.resolver_score = best_score
                linked += 1
                if trace:
                    token = trace.start("related_page_crawl", brand=item.brand, name=item.name, year=item.year, deadline=deadline)
                    trace.finish(token, linked_count=1, accepted_url=best.url, deadline=deadline)
                cache.put(item, source="related_known_pages")
            elif trace and best:
                trace.add_reject("related_page_crawl", best.url, "score_below_threshold", brand=item.brand, name=item.name, year=item.year, deadline=deadline)
        cache.save()
        if trace and branch_token:
            trace.finish(branch_token, result_count=len(discovered), linked_count=linked, skipped_reason="" if linked else "score_below_threshold", deadline=deadline)
        return linked
        
    @staticmethod
    def attach_external_search(
        scraper,
        candidates: list[UnifiedFragrance],
        cache: IdentityCache,
        budget_seconds: float = 5.0,
        max_workers: int = 3,
        trace: ResolverTrace | None = None,
    ) -> int:
        missing = [item for item in candidates if not item.frag_url and item.brand and item.name]
        if not missing or budget_seconds <= 0:
            if trace:
                trace.add_skip("external_search", "already_linked" if not missing else "budget_exhausted")
            return 0
        deadline = Deadline(budget_seconds)
        branch_token = trace.start("external_search", query=f"{len(missing)} missing", deadline=deadline) if trace else None
        linked = 0
        def resolve_one(item: UnifiedFragrance) -> None:
            nonlocal linked
            if deadline.expired():
                if trace:
                    trace.add_skip("external_search", "budget_exhausted", brand=item.brand, name=item.name, year=item.year, deadline=deadline)
                return
            url = SearchSniper.resolve(scraper, item.brand, item.name, deadline=deadline, trace=trace)
            if url:
                probe = CatalogItem(FragranticaEngine.name_from_url(url), FragranticaEngine.brand_from_url(url), "", url)
                score = Orchestrator.identity_score(item, probe)
                if score < Orchestrator.RELATED_ACCEPT:
                    if trace:
                        trace.add_reject("external_search", url, "score_below_threshold", brand=item.brand, name=item.name, year=item.year, deadline=deadline)
                    return
                item.frag_url = url
                item.resolver_source = "external_sniper"
                item.resolver_score = score
                cache.put(item, source="external_sniper")
                linked += 1
                if trace:
                    token = trace.start("external_search", brand=item.brand, name=item.name, year=item.year, deadline=deadline)
                    trace.finish(token, linked_count=1, accepted_url=url, deadline=deadline)
        run_budgeted_items(missing, resolve_one, seconds=budget_seconds, max_workers=max_workers)
        cache.save()
        if trace and branch_token:
            trace.finish(branch_token, linked_count=linked, skipped_reason="" if linked else "no_perfume_links_found", deadline=deadline)
        return linked
        
    @staticmethod
    def resolve(scraper, brand: str, name: str, deadline: Deadline | None = None, trace: ResolverTrace | None = None) -> str:
        """Opt-in last-resort web resolver. Keep disabled by default in first-load UX."""
        brand = TextSanitizer.clean(brand)
        name = TextSanitizer.clean(name)
        if not brand or not name:
            return ""
        search_term = f'site:fragrantica.com/perfume "{brand}" "{name}"'
        urls = [
            f"https://www.bing.com/search?q={quote(search_term)}",
            f"https://www.google.com/search?q={quote(search_term)}",
        ]
        for search_url in urls:
            if deadline and deadline.expired():
                break
            res = Http.get(
                scraper,
                search_url,
                timeout=3.0,
                deadline=deadline,
                attempts=1,
            )
            if not res:
                continue
            soup = BeautifulSoup(res.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                if "/url?q=" in href:
                    href = href.split("/url?q=", 1)[1].split("&", 1)[0]
                href = FragranticaEngine.canonical_url(href)
                if FragranticaEngine.is_perfume_url(href):
                    return href
        return ""

class BasenotesEngine:
    BASE_URL = "https://basenotes.com"
    _YEAR_PATTERN = re.compile(r"\((\b(?:17|18|19|20)\d{2}\b)\)")
    _YEAR_FALLBACK = re.compile(r"\b((?:17|18|19|20)\d{2})\b")
    _BY_SPLIT = re.compile(r"(?i)\s+by\s+")
    _CLEAN_CHARS = re.compile(r"[^a-zA-Z0-9\s\.\-\%\&'’]")
    _CONSENSUS_PATTERN = re.compile(r"(\d+)\s*(Positive|Neutral|Negative)\s*\((\d+)%\)", re.I)
    
    @staticmethod
    def extract_search_data(scraper, query: str, deadline: Deadline | None = None) -> list[UnifiedFragrance]:
        url = f"https://basenotes.com/directory/?sort=pop-desc&search={quote(query)}&type=fragrances&page=1"
        res = BasenotesEngine._get_with_retries(scraper, url, timeout=8, attempts=2, deadline=deadline)
        if not res:
            return []
        soup = BeautifulSoup(res.content, "html.parser", from_encoding="utf-8")
        results: dict[str, UnifiedFragrance] = {}
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if "/fragrances/" not in href or href.strip("/") == "fragrances":
                continue
            full_url = urljoin(url, href).split("?", 1)[0].split("#", 1)[0]
            parent = BasenotesEngine._nearest_context_parent(a)
            context = BasenotesEngine._context_text(parent) if parent else safe_get_text(a)
            title = TextSanitizer.clean(a.get("title") or safe_get_text(a))
            candidate = BasenotesEngine._parse_name_metadata(full_url, context, title)
            if candidate.name and candidate.brand:
                if full_url not in results:
                    results[full_url] = candidate
        return list(results.values())
        
    @staticmethod
    def _get_with_retries(
        scraper,
        url: str,
        timeout: float = 8,
        attempts: int = 2,
        deadline: Deadline | None = None,
    ):
        bn_scraper = get_basenotes_scraper(scraper)
        for attempt in range(max(1, attempts)):
            if deadline and deadline.expired():
                break
            try:
                request_timeout = deadline.timeout(timeout) if deadline else timeout
                res = bn_scraper.get(url, timeout=request_timeout)
                if _response_has_challenge(res):
                    if bn_scraper is not scraper:
                        reset_basenotes_scraper(clear_cache=True)
                        bn_scraper = get_basenotes_scraper(scraper)
                    time.sleep(random.uniform(0.75, 1.5))
                    continue
                if int(getattr(res, "status_code", 0) or 0) >= 400:
                    return None
                return res
            except Exception:
                if attempt >= attempts - 1:
                    break
                time.sleep(0.25)
        return None
        
    @staticmethod
    def _nearest_context_parent(node):
        parent = node.parent
        for _ in range(5):
            if not parent:
                return None
            if parent.name in {"div", "li", "article", "tr", "td", "section"}:
                return parent
            parent = parent.parent
        return node.parent
        
    @staticmethod
    def _context_text(node) -> str:
        if not node:
            return ""
        parts = [TextSanitizer.clean(t) for t in node.stripped_strings if TextSanitizer.clean(t)]
        return " | ".join(parts)
        
    @staticmethod
    def _parse_name_metadata(url: str, context: str, title_text: str) -> UnifiedFragrance:
        slug = url.rstrip("/").split("/")[-1]
        slug = re.sub(r"\.\d+$", "", slug)
        raw_name = TextSanitizer.title_from_slug(slug)
        name = raw_name
        brand = "Unknown"
        year = ""
        combined = f"{title_text} | {context}"
        year_match = BasenotesEngine._YEAR_PATTERN.search(combined)
        if not year_match:
            year_match = BasenotesEngine._YEAR_FALLBACK.search(title_text)
        if year_match:
            year = year_match.group(1)
            
        if " By " in raw_name:
            left, right = raw_name.split(" By ", 1)
            name = left.strip()
            brand = right.strip()
        elif re.search(r"(?i)\bby\b", combined):
            # Guard against split failure when using strict whitespace vs boundary tests
            parts = BasenotesEngine._BY_SPLIT.split(combined, maxsplit=1)
            if len(parts) == 2:
                name = parts[0].split("|")[-1].strip()
                brand = parts[1].split("|")[0].strip()
                name = BasenotesEngine._CLEAN_CHARS.sub("", name)
                brand = BasenotesEngine._CLEAN_CHARS.sub("", brand)
                
        if year:
            name = re.sub(rf"\b{re.escape(year)}\b", "", name)
            brand = re.sub(rf"\b{re.escape(year)}\b", "", brand)
        name = TextSanitizer.clean(re.sub(r"\(\s*\)", "", name))
        brand = TextSanitizer.clean(re.sub(r"\(\s*\)", "", brand))
        return UnifiedFragrance(name=name, brand=brand, year=year, bn_url=url)
        
    @staticmethod
    def fetch_metrics(scraper, frag: UnifiedFragrance, deadline: Deadline | None = None) -> None:
        if not frag.bn_url:
            return
        res = BasenotesEngine._get_with_retries(scraper, frag.bn_url, timeout=3.0, attempts=1, deadline=deadline)
        if not res:
            return
        positive = re.search(r"(\d+)\s*Positive\s*\((\d+)%\)", res.text, re.I)
        neutral = re.search(r"(\d+)\s*Neutral\s*\(\d+%\)", res.text, re.I)
        negative = re.search(r"(\d+)\s*Negative\s*\(\d+%\)", res.text, re.I)
        if positive:
            total = int(positive.group(1))
            if neutral:
                total += int(neutral.group(1))
            if negative:
                total += int(negative.group(1))
            frag.bn_vote_count = total
            frag.bn_positive_pct = int(positive.group(2))

    @staticmethod
    def _clean_note_items(text: str) -> list[str]:
        if not text:
            return []
        items: list[str] = []
        seen: set[str] = set()
        for item in re.split(r"[,;\|\n\u2022]", text):
            item = TextSanitizer.clean(item)
            if not item or re.match(r"(?i)^(and\s+)?(top|head|heart|middle|base)\s*(notes?)?$", item):
                continue
            item = re.sub(
                r"(?i)^(?:top|head|heart|middle|base)\s*(?:notes?)?\s*[:\-]?\s*",
                "",
                item,
            ).strip()
            if len(item) > 1 and item not in seen:
                items.append(item)
                seen.add(item)
        return items

    @staticmethod
    def _apply_text_to_notes(raw_text: str, notes_obj: NotesList) -> None:
        raw_text = TextSanitizer.clean(raw_text)
        if not raw_text:
            return

        if re.search(r"(?i)(top|head|heart|middle|base)\s*notes?\s*[:\-]?", raw_text):
            top_match = re.search(
                r"(?i)(?:top|head)\s*notes?[\s:\-]*(.*?)(?=(?:heart|middle|base)\s*notes?|$)",
                raw_text,
                re.S,
            )
            heart_match = re.search(
                r"(?i)(?:heart|middle)\s*notes?[\s:\-]*(.*?)(?=(?:base)\s*notes?|$)",
                raw_text,
                re.S,
            )
            base_match = re.search(r"(?i)(?:base)\s*notes?[\s:\-]*(.*)", raw_text, re.S)

            if top_match:
                notes_obj.top.extend(BasenotesEngine._clean_note_items(top_match.group(1)))
                notes_obj.has_pyramid = True
            if heart_match:
                notes_obj.heart.extend(BasenotesEngine._clean_note_items(heart_match.group(1)))
                notes_obj.has_pyramid = True
            if base_match:
                notes_obj.base.extend(BasenotesEngine._clean_note_items(base_match.group(1)))
                notes_obj.has_pyramid = True
        else:
            notes_obj.flat.extend(BasenotesEngine._clean_note_items(raw_text))
            
    @staticmethod
    def fetch_details(
        scraper,
        url: str,
        unified_details: UnifiedDetails,
        deadline: Deadline | None = None,
    ) -> None:
        if not url:
            return
        res = BasenotesEngine._get_with_retries(scraper, url, timeout=5.0, attempts=1, deadline=deadline)
        if not res:
            return
        soup = BeautifulSoup(res.content, "html.parser", from_encoding="utf-8")
        header_inner = soup.find(class_=re.compile(r"fraghead-inner", re.I)) or soup
        gender_span = header_inner.find("span", class_=re.compile(r"h1_gender", re.I))
        if gender_span:
            span_html = str(gender_span).replace(" ", "").lower()
            if "dodgerblue" in span_html and "hotpink" in span_html:
                unified_details.gender = "Unisex"
            elif "dodgerblue" in span_html:
                unified_details.gender = "Masculine"
            elif "hotpink" in span_html:
                unified_details.gender = "Feminine"
        desc_div = soup.find("div", itemprop="description") or soup.find(
            "div", class_=re.compile(r"fragdesc", re.I)
        )
        if desc_div:
            unified_details.description = safe_get_text(desc_div, separator="\n")
        reviews_container = soup.find(id="fragrev") or soup.find(
            "div", class_=re.compile(r"fragreviews", re.I)
        )
        if reviews_container:
            for a in reviews_container.find_all("a"):
                text = safe_get_text(a)
                match = BasenotesEngine._CONSENSUS_PATTERN.search(text)
                if match:
                    unified_details.bn_consensus[match.group(2).lower()[:3]] = (
                        int(match.group(1)),
                        int(match.group(3)),
                    )
        
        if not unified_details.notes.has_pyramid:
            ul_container = soup.find("ul", class_="fragrancenotes")
            if ul_container:
                list_items = ul_container.find_all("li", recursive=False)
                if list_items and any(item.find("h3") for item in list_items):
                    for item in list_items:
                        heading = item.find("h3")
                        nested_ul = item.find("ul")
                        if not heading or not nested_ul:
                            continue
                        label = TextSanitizer.clean(heading.get_text(strip=True).lower())
                        notes = BasenotesEngine._clean_note_items(
                            nested_ul.get_text(separator=", ", strip=True)
                        )
                        if "head" in label or "top" in label:
                            unified_details.notes.top.extend(notes)
                            unified_details.notes.has_pyramid = True
                        elif "heart" in label or "middle" in label:
                            unified_details.notes.heart.extend(notes)
                            unified_details.notes.has_pyramid = True
                        elif "base" in label:
                            unified_details.notes.base.extend(notes)
                            unified_details.notes.has_pyramid = True
                else:
                    BasenotesEngine._apply_text_to_notes(
                        ul_container.get_text(separator="\n", strip=True),
                        unified_details.notes,
                    )
            else:
                div_container = soup.find("div", class_="fragrancenotes")
                if div_container:
                    BasenotesEngine._apply_text_to_notes(
                        div_container.get_text(separator="\n", strip=True),
                        unified_details.notes,
                    )
                else:
                    notes_header = soup.find(
                        lambda tag: tag.name in {"h2", "h3", "h4", "div", "b", "strong", "span"}
                        and re.match(
                            r"^(?:fragrance\s+)?notes$",
                            TextSanitizer.clean(tag.get_text(strip=True)),
                            re.I,
                        )
                    )
                    if notes_header:
                        blocks: list[str] = []
                        stop_markers = (
                            "latest reviews",
                            "your tags",
                            "by the same house",
                            "advertisement",
                            "where to buy",
                            "add your review",
                            "fragrance reviews",
                            "similar fragrances",
                        )
                        for sibling in notes_header.find_next_siblings(limit=15):
                            sibling_text = TextSanitizer.clean(
                                sibling.get_text(separator=" ", strip=True).lower()
                            )
                            if any(marker in sibling_text for marker in stop_markers):
                                break
                            blocks.append(TextSanitizer.clean(sibling.get_text(separator="\n", strip=True)))
                        BasenotesEngine._apply_text_to_notes(
                            "\n".join(blocks).strip(),
                            unified_details.notes,
                        )
                    elif soup.body:
                        match = re.search(
                            r"(?i)^(?:fragrance\s+)?notes\s*[\r\n]+(.*?)(?=(?:latest reviews|your tags|by the same house|advertisement|where to buy|add your review|fragrance reviews|similar fragrances)|$)",
                            soup.body.get_text(separator="\n", strip=True),
                            re.I | re.S | re.M,
                        )
                        if match:
                            BasenotesEngine._apply_text_to_notes(match.group(1).strip(), unified_details.notes)

        for review_outer in soup.find_all("div", class_=re.compile(r"fragreviewouter", re.I)):
            for a in review_outer.find_all("a", string=re.compile(r"(?i)show all reviews")):
                a.decompose()
            text = review_outer.get_text(separator=" | ", strip=True)
            text = re.sub(r"(?:\s*\|\s*)+", " | ", text).strip(" |")
            text = TextSanitizer.clean(text)
            if text:
                unified_details.reviews.append(Review(text=text, source="Basenotes"))

class FragranticaEngine:
    BASE_URL = "https://www.fragrantica.com"
    SEARCH_URL = BASE_URL + "/search/?query={query}"
    YEAR_RE = re.compile(r"\b(?:17|18|19|20)\d{2}\b")
    PERFUME_URL_RE = re.compile(r"^https://www\.fragrantica\.com/perfume/[^/]+/[^?#]+-\d+\.html$")
    
    # Global exclusion strings to prevent shopping links or screen reader text 
    # from being mistakenly ingested as bar labels or metric text.
    BAD_UI_TEXTS = (
        "frequency", "percentage", "right now", "...", "votes", "read more",
        "items on", "search on", "buy ", "ebay", "amazon", "fragrancenet", "off retail"
    )

    # Vocabulary of canonical-label aliases per card. First entry of each group
    # is the canonical display label; remaining entries are equivalents
    # (translation/word variants). Lookup goes through
    # TextSanitizer.normalize_identity, so spacing/punctuation/case differences
    # collapse automatically — entries only need to cover real *word* variants
    # (e.g. "Eternal" vs "Forever", "Fall" vs "Autumn").
    KNOWN_METRIC_LABELS: dict[str, list[list[str]]] = {
        "longevity": [
            ["Very Weak"],
            ["Weak", "Soft"],
            ["Moderate"],
            ["Long Lasting", "Long-Lasting", "Longlasting"],
            ["Eternal"],
        ],
        "sillage": [
            ["Intimate", "Close"],
            ["Moderate"],
            ["Strong", "Heavy"],
            ["Enormous", "Room Filling"],
            # Legacy/shared Fragrantica label family. Keep separate so the
            # parser preserves the actual page label when this family appears.
            ["Very Weak"],
            ["Weak", "Soft"],
            ["Long Lasting", "Long-Lasting", "Longlasting"],
            ["Eternal"],
        ],
        "projection": [
            ["Intimate", "Close"],
            ["Moderate"],
            ["Strong", "Heavy"],
            ["Enormous", "Room Filling"],
        ],
        "gender": [
            ["Female"],
            ["More Female", "More Feminine"],
            ["Unisex"],
            ["More Male", "More Masculine"],
            ["Male"],
        ],
        "price value": [
            ["Way Overpriced"],
            ["Overpriced"],
            ["Ok", "Okay"],
            ["Good Value"],
            ["Great Value"],
        ],
        "value": [
            ["Way Overpriced"],
            ["Overpriced"],
            ["Ok", "Okay"],
            ["Good Value"],
            ["Great Value"],
        ],
        "when to wear": [
            ["Winter"],
            ["Spring"],
            ["Summer"],
            ["Fall", "Autumn"],
            ["Day"],
            ["Night"],
            ["Day or Night"],
        ],
        "season": [
            ["Winter"],
            ["Spring"],
            ["Summer"],
            ["Fall", "Autumn"],
            ["Day"],
            ["Night"],
            ["Day or Night"],
        ],
    }

    @staticmethod
    def _fg_status_passphrase(host: str) -> str:
        """
        Python equivalent of Fragrantica module 2616 _pd() passphrase derivation.

        JS source logic:
        host = window.location.host
        reversed_host = host.split("").reverse().join("")
        mixed = host[0] + reversed_host[0] + host[1] + reversed_host[1] ...
        obfuscated = chars XOR ((7*i + 13) & 127)
        o = MD5(obfuscated)
        s = MD5(host + o[:8])
        passphrase = MD5(o + s)
        """
        host = (host or "www.fragrantica.com").strip()
        rev = host[::-1]

        mixed = ""
        for i in range(len(host)):
            mixed += host[i] + rev[i]

        obfuscated = ""
        for i, ch in enumerate(mixed):
            obfuscated += chr(ord(ch) ^ ((7 * i + 13) & 127))

        o = hashlib.md5(obfuscated.encode("utf-8")).hexdigest()
        s = hashlib.md5((host + o[:8]).encode("utf-8")).hexdigest()
        return hashlib.md5((o + s).encode("utf-8")).hexdigest()

    @staticmethod
    def _evp_bytes_to_key_md5(password: bytes, salt: bytes, key_len: int = 32, iv_len: int = 16) -> tuple[bytes, bytes]:
        """
        CryptoJS/OpenSSL-compatible passphrase KDF.
        CryptoJS AES.decrypt(cipherParams, passphrase, {format}) uses this style
        when the key is a string passphrase.
        """
        out = b""
        prev = b""

        while len(out) < key_len + iv_len:
            prev = hashlib.md5(prev + password + salt).digest()
            out += prev

        return out[:key_len], out[key_len:key_len + iv_len]

    @staticmethod
    def _aes_cbc_decrypt_pkcs7(ciphertext: bytes, key: bytes, iv: bytes) -> bytes:
        """
        Decrypt AES-CBC with PKCS#7 padding.

        Uses pycryptodome if available, otherwise cryptography if available.
        This avoids hard-adding a new dependency to the parser.
        """
        raw = None

        try:
            from Crypto.Cipher import AES as PyCryptoAES  # pycryptodome
            raw = PyCryptoAES.new(key, PyCryptoAES.MODE_CBC, iv).decrypt(ciphertext)
        except Exception:
            raw = None

        if raw is None:
            try:
                from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
                from cryptography.hazmat.backends import default_backend

                cipher = Cipher(
                    algorithms.AES(key),
                    modes.CBC(iv),
                    backend=default_backend(),
                )
                decryptor = cipher.decryptor()
                raw = decryptor.update(ciphertext) + decryptor.finalize()
            except Exception as exc:
                raise RuntimeError(
                    "AES decrypt requires either pycryptodome or cryptography. "
                    "Install one, or decode status in browser with await _pd(status)."
                ) from exc

        if not raw:
            return b""

        pad = raw[-1]
        if pad < 1 or pad > 16:
            raise ValueError("Invalid PKCS#7 padding from decrypted status payload.")

        if raw[-pad:] != bytes([pad]) * pad:
            raise ValueError("Invalid PKCS#7 padding bytes from decrypted status payload.")

        return raw[:-pad]

    @staticmethod
    def extract_encrypted_status_payload(soup: BeautifulSoup) -> dict[str, Any] | None:
        """
        Extracts:
            let status = {"ct":"...","iv":"...","s":"..."};
        from Fragrantica inline scripts.
        """
        if soup is None:
            return None

        status_re = re.compile(
            r"""(?:let|var|const)\s+status\s*=\s*(\{.*?\})\s*;""",
            re.I | re.DOTALL,
        )

        for script in soup.find_all("script"):
            if script.get("src"):
                continue

            raw = script.string or script.get_text() or ""
            if not raw:
                continue

            m = status_re.search(raw)
            if not m:
                continue

            payload_raw = m.group(1)

            try:
                payload = json.loads(payload_raw)
            except Exception:
                try:
                    payload = json.loads(payload_raw.replace("\\/", "/"))
                except Exception:
                    continue

            if all(k in payload for k in ("ct", "iv", "s")):
                return payload

        return None

    @staticmethod
    def decode_fragrantica_status_payload(payload: dict[str, Any], page_url: str = "") -> dict[str, Any] | None:
        """
        Python equivalent of:
            await _pd(status)

        Decrypts the inline Fragrantica status payload into status_data.
        """
        if not payload:
            return None

        ct = str(payload.get("ct", "") or "")
        salt_hex = str(payload.get("s", "") or "")
        iv_hex = str(payload.get("iv", "") or "")

        if not ct or not salt_hex:
            return None

        host = urlparse(page_url or FragranticaEngine.BASE_URL).netloc or "www.fragrantica.com"
        passphrase = FragranticaEngine._fg_status_passphrase(host)

        ciphertext = base64.b64decode(ct)
        salt = bytes.fromhex(salt_hex)

        key, derived_iv = FragranticaEngine._evp_bytes_to_key_md5(
            passphrase.encode("utf-8"),
            salt,
            key_len=32,
            iv_len=16,
        )

        # CryptoJS password-based decrypt derives IV from salt.
        # Keep payload IV as fallback only.
        iv_candidates = [derived_iv]

        if iv_hex:
            try:
                payload_iv = bytes.fromhex(iv_hex)
                if payload_iv not in iv_candidates:
                    iv_candidates.append(payload_iv)
            except Exception:
                pass

        last_error: Exception | None = None

        for iv in iv_candidates:
            try:
                plaintext = FragranticaEngine._aes_cbc_decrypt_pkcs7(ciphertext, key, iv)
                text = plaintext.decode("utf-8", errors="strict").strip()
                if text:
                    return json.loads(text)
            except Exception as exc:
                last_error = exc
                continue

        if last_error:
            raise last_error

        return None

    
    
    
 


    







    @staticmethod
    def _card_key_for_title(card_name: str) -> str:
        if not card_name:
            return ""
        low = card_name.lower()
        # Punctuation-stripped compact form collapses "Price/Value",
        # "price_value", "pricevalue", and "Price Value" to one token so
        # callers don't have to special-case the slash/underscore variants.
        compact = re.sub(r"[^a-z]", "", low)
        compact_map = {
            "pricevalue": "price value",
            "valueformoney": "price value",
            "longevity": "longevity",
            "sillage": "sillage",
            "projection": "projection",
            "gender": "gender",
            "whentowear": "when to wear",
            "season": "season",
        }
        if compact in compact_map:
            return compact_map[compact]
        for key in FragranticaEngine.KNOWN_METRIC_LABELS:
            if key in low:
                return key
        return ""

    @staticmethod
    def _canonical_label(card_key: str, parsed_text: str) -> tuple[str, str]:
        """Resolve a parsed label string to (canonical_label, alias_norm).
        Returns ("", "") when no alias-group matches — callers should fall back
        to the cleaned parsed text."""
        if not parsed_text or card_key not in FragranticaEngine.KNOWN_METRIC_LABELS:
            return "", ""
        norm = TextSanitizer.normalize_identity(parsed_text)
        if not norm:
            return "", ""
        # Exact normalized match first.
        for group in FragranticaEngine.KNOWN_METRIC_LABELS[card_key]:
            for alias in group:
                alias_norm = TextSanitizer.normalize_identity(alias)
                if alias_norm and norm == alias_norm:
                    return group[0], alias_norm
        # Whole-token containment, longest alias first so "More Female" beats "Female".
        candidates: list[tuple[int, str, str]] = []
        for group in FragranticaEngine.KNOWN_METRIC_LABELS[card_key]:
            for alias in group:
                alias_norm = TextSanitizer.normalize_identity(alias)
                if not alias_norm:
                    continue
                if (
                    norm == alias_norm
                    or norm.startswith(alias_norm + " ")
                    or norm.endswith(" " + alias_norm)
                    or (" " + alias_norm + " ") in norm
                ):
                    candidates.append((len(alias_norm), group[0], alias_norm))
        if candidates:
            candidates.sort(reverse=True)
            return candidates[0][1], candidates[0][2]
        return "", ""
    
    @staticmethod
    def extract_search_data(
        scraper,
        query: str,
        max_results: int = 25,
        deadline: Deadline | None = None,
    ) -> list[UnifiedFragrance]:
        url = FragranticaEngine.SEARCH_URL.format(query=quote(query))
        for _ in range(2):
            if deadline and deadline.expired():
                break
            res = Http.get(
                scraper,
                url,
                timeout=6.0,
                referer=FragranticaEngine.BASE_URL,
                deadline=deadline,
                attempts=1,
            )
            if not res:
                continue
            results = FragranticaEngine.parse_search_html(res.text, max_results=max_results)
            if results:
                return results
            time.sleep(0.15)
        return []
        
    @staticmethod
    def parse_search_html(markup: str, max_results: int = 25) -> list[UnifiedFragrance]:
        if not markup:
            return []
        soup = BeautifulSoup(markup, "html.parser")
        cards = FragranticaEngine.find_result_cards(soup)
        results: list[UnifiedFragrance] = []
        seen: set[str] = set()
        for card in cards:
            item = FragranticaEngine.parse_result_card(card)
            if not item:
                continue
            key = FragranticaEngine.dedupe_key(item.frag_url)
            if key in seen:
                continue
            seen.add(key)
            item.resolver_source = "fragrantica_native_search"
            item.resolver_score = 0.65
            results.append(item)
            if max_results and len(results) >= max_results:
                break
        return results
        
    @staticmethod
    def find_result_cards(soup: BeautifulSoup) -> list:
        root = (
            soup.find(class_=lambda value: FragranticaEngine.class_has(value, "ais-StateResults"))
            or soup.find(class_=lambda value: FragranticaEngine.class_has(value, "ais-Hits"))
            or soup.find("div", class_=lambda value: FragranticaEngine.class_has(value, "grid-cols-3"))
            or soup
        )
        cards = root.find_all(
            "a",
            href=True,
            class_=lambda value: FragranticaEngine.class_has(value, "prefumeHbox"),
        )
        if cards:
            return cards
        fallback_cards = []
        for a in root.find_all("a", href=True):
            href = a.get("href", "").strip()
            canonical = FragranticaEngine.canonical_url(href)
            if not FragranticaEngine.is_perfume_url(canonical):
                continue
            has_identity = bool(
                a.find("h3")
                or a.find("p")
                or a.get("title")
                or a.get("aria-label")
                or a.find("img", alt=True)
            )
            if has_identity:
                fallback_cards.append(a)
        return fallback_cards
        
    @staticmethod
    def parse_result_card(card) -> UnifiedFragrance | None:
        href = TextSanitizer.clean(card.get("href", ""))
        frag_url = FragranticaEngine.canonical_url(href)
        if not FragranticaEngine.is_perfume_url(frag_url):
            return None
        title = TextSanitizer.clean(card.get("title", ""))
        aria = TextSanitizer.clean(card.get("aria-label", ""))
        label = title or aria
        if label.lower().startswith("link to "):
            label = label[8:].strip()
        name = FragranticaEngine.extract_card_name(card) or FragranticaEngine.name_from_url(frag_url)
        brand = FragranticaEngine.extract_card_brand(card, name, label, frag_url)
        year = FragranticaEngine.extract_card_year(card, label)
        name = TextSanitizer.clean(name)
        brand = TextSanitizer.clean(brand)
        year = TextSanitizer.clean(year)
        if not name or not brand:
            return None
        return UnifiedFragrance(name=name, brand=brand, year=year, frag_url=frag_url)
        
    @staticmethod
    def extract_card_name(card) -> str:
        h3 = card.find("h3")
        if h3:
            text = safe_get_text(h3)
            if text:
                return text
        img = card.find("img")
        if img:
            alt = TextSanitizer.clean(img.get("alt", ""))
            alt = re.sub(r"(?i)^perfume\s+", "", alt).strip()
            if alt:
                return alt
        return ""
        
    @staticmethod
    def extract_card_brand(card, name: str, label: str, url: str) -> str:
        h3 = card.find("h3")
        if h3:
            p = h3.find_next_sibling("p")
            if p:
                text = safe_get_text(p)
                if text:
                    return text

        if label and name:
            cleaned = FragranticaEngine.YEAR_RE.sub("", label)
            cleaned = re.sub(r"\b(?:male|female|unisex|men|women)\b", "", cleaned, flags=re.I)
            cleaned = TextSanitizer.clean(cleaned)
            if cleaned.lower().endswith(name.lower()):
                brand = cleaned[: -len(name)].strip()
                if brand:
                    return brand
        return FragranticaEngine.brand_from_url(url)
        
    @staticmethod
    def extract_card_year(card, label: str = "") -> str:
        bottom_container = card.find("div", class_=lambda v: FragranticaEngine.class_has(v, "border-t"))
        if bottom_container:
            for span in bottom_container.find_all("span"):
                text = safe_get_text(span)
                if FragranticaEngine.YEAR_RE.fullmatch(text):
                    return text
                    
        for span in card.find_all("span", class_=lambda v: FragranticaEngine.class_has(v, "font-medium")):
            text = safe_get_text(span)
            if FragranticaEngine.YEAR_RE.fullmatch(text):
                return text

        for span in card.find_all("span"):
            text = safe_get_text(span)
            if FragranticaEngine.YEAR_RE.fullmatch(text):
                return text
                
        if label:
            match = FragranticaEngine.YEAR_RE.search(label)
            if match:
                return match.group(0)
        return ""
        
    @staticmethod
    def canonical_url(href: str) -> str:
        href = html.unescape(href or "").strip()
        if not href:
            return ""
        url = urljoin(FragranticaEngine.BASE_URL, href)
        url = url.split("#", 1)[0].split("?", 1)[0]
        url = url.replace("http://www.fragrantica.com", "https://www.fragrantica.com")
        url = url.replace("http://fragrantica.com", "https://www.fragrantica.com")
        url = url.replace("https://fragrantica.com", "https://www.fragrantica.com")
        return url
        
    @staticmethod
    def is_perfume_url(url: str) -> bool:
        return bool(FragranticaEngine.PERFUME_URL_RE.match(url or ""))
        
    @staticmethod
    def dedupe_key(url: str) -> str:
        match = re.search(r"/[^/]+-(\d+)\.html$", url or "")
        if match:
            return match.group(1)
        return url or ""
        
    @staticmethod
    def class_has(value, wanted: str) -> bool:
        if not value:
            return False
        if isinstance(value, (list, tuple, set)):
            return wanted in value
        return wanted in str(value).split()
        
    @staticmethod
    def brand_from_url(url: str) -> str:
        match = re.search(r"/perfume/([^/]+)/", url or "")
        if not match:
            return ""
        return TextSanitizer.title_from_slug(match.group(1))
        
    @staticmethod
    def name_from_url(url: str) -> str:
        match = re.search(r"/perfume/[^/]+/([^/]+)-(\d+)\.html$", url or "")
        if not match:
            return ""
        return TextSanitizer.title_from_slug(match.group(1))
        
    # ------------------------------------------------------------------
    # Metric extraction: multi-pass pipeline.
    # ------------------------------------------------------------------

    PRICE_VALUE_DISPLAY = {
        "Way Overpriced": "$$$$",
        "Overpriced": "$$$",
        "Ok": "$$",
        "Good Value": "$",
        "Great Value": "$",
    }
    
    PERFORMANCE_CARD_NAMES = {
        "longevity": "Longevity",
        "sillage": "Sillage",
        "pricevalue": "Price Value",
    }
    

    

    
    @staticmethod
    def fetch_details(
        scraper,
        url: str,
        unified_details: UnifiedDetails,
        deadline: Deadline | None = None,
    ) -> None:
        if not url:
            return
        res = Http.get(scraper, url, timeout=5.0, referer=FragranticaEngine.BASE_URL, deadline=deadline, attempts=1)
        if not res:
            return
        soup = BeautifulSoup(res.content, "html.parser", from_encoding="utf-8")

        decoded_status = None
        status_payload = FragranticaEngine.extract_encrypted_status_payload(soup)

        if status_payload:
            
            decoded_status = FragranticaEngine.decode_fragrantica_status_payload(status_payload, url)
                
            

        status_metric_cards = FragranticaEngine.extract_status_metric_cards(decoded_status)

        for card_name, metrics in status_metric_cards.items():
            if metrics:
                unified_details.frag_cards[card_name] = metrics


        
        
        



        

            


            


        
        
        # Note Pyramids (using the new strict pyramid-note-link class architecture)
        top = FragranticaEngine.extract_notes_after_label(soup, "Top Notes")
        heart = FragranticaEngine.extract_notes_after_label(soup, "Middle Notes") or FragranticaEngine.extract_notes_after_label(soup, "Heart Notes")
        base = FragranticaEngine.extract_notes_after_label(soup, "Base Notes")

        if top or heart or base:
            unified_details.notes.has_pyramid = True
            unified_details.notes.top.extend(top)
            unified_details.notes.heart.extend(heart)
            unified_details.notes.base.extend(base)
        else:
            flat = FragranticaEngine.extract_flat_notes(soup)
            if flat and not unified_details.notes.flat:
                unified_details.notes.flat = flat

        # Main Rating Summary
        rating = FragranticaEngine.extract_rating_summary(soup)
        if rating:
            unified_details.frag_cards.setdefault("Fragrantica Rating", []).append(rating)

        # Surgical Main Accords extraction
        accords = FragranticaEngine.extract_main_accords(soup)
        if accords:
            unified_details.frag_cards.setdefault("Main accords", []).extend(accords)

        # Dedicated performance extraction for Fragrantica performance cards.
        # Main Accords stays fully isolated above and is not touched here.
        if not status_performance_cards:
            performance_cards = FragranticaEngine.extract_performance_metrics(soup)
            for card_name, metrics in performance_cards.items():
                if metrics:
                    unified_details.frag_cards[card_name] = metrics
        # Existing generic path stays available only for non-performance cards.
        # Longevity/Sillage/Price Value must not fall back to whole-page scans.
        target_titles = ["Gender", "When to wear", "Projection"]
        label_nodes: dict[str, Any] = {}
        for title in target_titles:
            node = FragranticaEngine._find_card_title_node(soup, title)
            if node is not None:
                label_nodes[title] = node

        json_raw = FragranticaEngine.extract_metrics_from_json(soup)
        json_pass = {
            card: metrics
            for card, metrics in json_raw.items()
            if card in target_titles
        }

        dom_pass: dict[str, list[dict[str, Any]]] = {}
        text_pass: dict[str, list[dict[str, Any]]] = {}
        for title, node in label_nodes.items():
            others = [n for t, n in label_nodes.items() if t != title]
            container = FragranticaEngine.find_tightest_card_container(node, others)
            if container is None:
                continue

            dom_metrics = FragranticaEngine.extract_generic_rating_card(container, title)
            if dom_metrics:
                dom_pass[title] = dom_metrics

            card_key = FragranticaEngine._card_key_for_title(title)
            text_metrics = FragranticaEngine.extract_metrics_from_text(container, card_key)
            if text_metrics:
                text_pass[title] = text_metrics

        merged = FragranticaEngine.merge_metric_passes([json_pass, dom_pass, text_pass])
        for card_name, metrics in merged.items():
            if metrics:
                unified_details.frag_cards[card_name] = metrics

        # Legacy tw-rating-card fallback for non-performance cards only.
        # If #performance is missing, Longevity/Sillage/Price Value should remain absent.
        if not any(t in merged for t in target_titles):
            for card in soup.find_all("div", class_=re.compile(r"tw-rating-card", re.I)):
                label_tag = card.find(class_=re.compile(r"tw-rating-card-label", re.I))
                card_name = safe_get_text(label_tag) if label_tag else ""
                compact_name = re.sub(r"[^a-z]", "", (card_name or "").lower())

                if compact_name in FragranticaEngine.PERFORMANCE_CARD_NAMES:
                    continue

                metrics = FragranticaEngine.extract_rating_card_metrics(card, card_name)
                if card_name and metrics:
                    unified_details.frag_cards[card_name] = metrics

        unified_details.pros_cons.extend(FragranticaEngine.extract_people_say(soup, unified_details.pros_cons))
        unified_details.reviews.extend(FragranticaEngine.extract_reviews(soup))

    @staticmethod
    def extract_main_accords(soup: BeautifulSoup) -> list[dict[str, Any]]:
        """Surgical extraction dedicated to Main Accords, actively bypassing hidden screen reader strings."""
        header = soup.find(lambda tag: getattr(tag, "name", None) in {"h2", "h3", "h4", "div", "span", "b", "p"} and "main accords" in tag.get_text(" ", strip=True).lower())
        if not header:
            return []
            
        container = FragranticaEngine.find_tightest_card_container(header, [])
        if not container:
            container = header.parent
            for _ in range(5):
                if not container:
                    break
                if container.find("div", style=re.compile(r"width\s*:\s*[\d.]+%", re.I)):
                    break
                container = container.parent
                
        if not container:
            return []
            
        metrics = []
        seen = set()
        bars = container.find_all("div", style=re.compile(r"width\s*:\s*[\d.]+%", re.I))
        
        for bar in bars:
            if bar.find("div", style=re.compile(r"width\s*:\s*[\d.]+%", re.I)):
                continue
                
            pct = FragranticaEngine.bar_pct_from_node(bar)
            if pct is None:
                continue
                
            label = ""
            row = bar
            for _ in range(4):
                if row is None or row is container:
                    break
                    
                texts = [TextSanitizer.clean(s) for s in row.stripped_strings if s]
                
                # POISON PILL: If the row contains ANY ad or shopping text, the entire row is skipped
                if any(any(bad in t.lower() for bad in FragranticaEngine.BAD_UI_TEXTS) for t in texts):
                    break
                    
                valid = []
                for t in texts:
                    if re.match(r"^[\d.,%()]+$", t):
                        continue
                    # Hard-skip tiny connector words that slip past the phrase blocks
                    if t.lower() in {"or", "and", "at", "on", "in", "by", "for", "buy"}:
                        continue
                    valid.append(t)
                    
                if valid:
                    label = valid[0].title()
                    break
                    
                row = row.parent
                
            if label and label not in seen:
                metrics.append({
                    "label": label,
                    "display": label,
                    "pct": pct,
                    "count": "",
                    "source": "Fragrantica"
                })
                seen.add(label)
                
        return metrics

    @staticmethod
    def _find_card_title_node(soup: BeautifulSoup, title: str):
        """Locate the heading node for a metric card. Lenient: normalize-identity
        comparison with whole-token containment so emoji/colon/icon suffixes don't
        break the match. Length cap is 60 chars."""
        title_norm = TextSanitizer.normalize_identity(title)
        if not title_norm:
            return None

        def looks_like_title(tag) -> bool:
            if getattr(tag, "name", None) not in {"h2", "h3", "h4", "h5", "div", "span", "b", "p"}:
                return False
            text = tag.get_text(" ", strip=True)
            if not text or len(text) > 60:
                return False
            norm = TextSanitizer.normalize_identity(text)
            if not norm:
                return False
            return (
                norm == title_norm
                or norm.startswith(title_norm + " ")
                or norm.endswith(" " + title_norm)
            )

        return soup.find(looks_like_title)



    @staticmethod
    def extract_status_metric_cards(decoded_status: dict[str, Any] | None) -> dict[str, list[dict[str, Any]]]:
        """
        Build Fragrantica metric cards from decoded status_data.

        Primary source:
            decoded_status["status"]

        Important:
            decoded_status["user_status"] is NOT public community data.
            It only represents the current user's own vote state.

        Produces:
            - Longevity
            - Sillage
            - Price Value
            - When to wear
            - Community Interest
            - Rating Distribution
        """
        if not isinstance(decoded_status, dict):
            return {}

        status = decoded_status.get("status")
        if not isinstance(status, dict):
            return {}

        def safe_int(value: Any) -> int:
            try:
                return int(float(str(value).replace(",", "").strip() or "0"))
            except (TypeError, ValueError):
                return 0

        def safe_float(value: Any) -> float:
            try:
                return float(str(value).replace(",", "").strip() or "0")
            except (TypeError, ValueError):
                return 0.0

        def distribution_card(
            *,
            card_name: str,
            dist_key: str,
            sum_key: str,
            avg_key: str = "",
            labels: dict[str, str],
            price_display: bool = False,
            source: str = "Fragrantica-Status",
        ) -> list[dict[str, Any]]:
            dist = status.get(dist_key)
            if not isinstance(dist, dict):
                return []

            rows: list[tuple[str, str, int]] = []
            for value_key, label in labels.items():
                rows.append((value_key, label, safe_int(dist.get(value_key, 0))))

            total_votes = safe_int(status.get(sum_key))
            if total_votes <= 0:
                total_votes = sum(count for _, _, count in rows)

            if total_votes <= 0:
                return []

            dominant_value, dominant_label, dominant_count = max(rows, key=lambda item: item[2])
            average = safe_float(status.get(avg_key)) if avg_key else 0.0

            metrics: list[dict[str, Any]] = []
            for value_key, label, count in rows:
                pct = round((count / total_votes) * 100.0, 2) if total_votes else 0.0

                display = label
                if price_display:
                    dollar = FragranticaEngine.PRICE_VALUE_DISPLAY.get(label, label)
                    display = f"{dollar} · {label}"

                metrics.append({
                    "label": label,
                    "display": display,
                    "value": int(value_key),
                    "pct": pct,
                    "count": str(count),
                    "source": source,
                    "dominant": label == dominant_label,
                    "dominant_label": dominant_label,
                    "dominant_count": str(dominant_count),
                    "total_votes": total_votes,
                    "average": average,
                })

            return metrics

        def max_normalized_card(
            *,
            card_name: str,
            count_keys: list[tuple[str, str]],
            source: str = "Fragrantica-Status",
        ) -> list[dict[str, Any]]:
            """
            For independent count signals such as seasons/day/night.

            These are not a closed 100% distribution because users can vote for
            multiple seasons or time windows. Therefore pct is max-normalized
            for visual bar strength:
                count / max_count * 100
            """
            rows = [(key, label, safe_int(status.get(key, 0))) for key, label in count_keys]
            rows = [row for row in rows if row[2] > 0]

            if not rows:
                return []

            max_count = max(count for _, _, count in rows)
            total_count = sum(count for _, _, count in rows)
            dominant_key, dominant_label, dominant_count = max(rows, key=lambda item: item[2])

            metrics: list[dict[str, Any]] = []
            for idx, (key, label, count) in enumerate(rows, 1):
                pct = round((count / max_count) * 100.0, 2) if max_count else 0.0

                metrics.append({
                    "label": label,
                    "display": label,
                    "value": key,
                    "pct": pct,
                    "count": str(count),
                    "source": source,
                    "dominant": label == dominant_label,
                    "dominant_label": dominant_label,
                    "dominant_count": str(dominant_count),
                    "total_votes": total_count,
                    "max_count": max_count,
                    "normalization": "max",
                })

            return metrics

        cards: dict[str, list[dict[str, Any]]] = {}

        # 1. Performance metrics: true vote distributions.
        longevity = distribution_card(
            card_name="Longevity",
            dist_key="longevity",
            sum_key="longevity_sum",
            avg_key="longevity_average",
            labels={
                "1": "Very Weak",
                "2": "Weak",
                "3": "Moderate",
                "4": "Long Lasting",
                "5": "Eternal",
            },
        )
        if longevity:
            cards["Longevity"] = longevity

        sillage = distribution_card(
            card_name="Sillage",
            dist_key="sillage",
            sum_key="sillage_sum",
            avg_key="sillage_average",
            labels={
                "1": "Intimate",
                "2": "Moderate",
                "3": "Strong",
                "4": "Enormous",
            },
        )
        if sillage:
            cards["Sillage"] = sillage

        price_value = distribution_card(
            card_name="Price Value",
            dist_key="price_value",
            sum_key="price_value_sum",
            avg_key="price_value_average",
            labels={
                "1": "Way Overpriced",
                "2": "Overpriced",
                "3": "Ok",
                "4": "Good Value",
                "5": "Great Value",
            },
            price_display=True,
        )
        if price_value:
            cards["Price Value"] = price_value

        # 2. When to wear: independent count signals, max-normalized.
        when_to_wear = max_normalized_card(
            card_name="When to wear",
            count_keys=[
                ("winter", "Winter"),
                ("spring", "Spring"),
                ("summer", "Summer"),
                ("autumn", "Autumn"),
                ("day", "Day"),
                ("night", "Night"),
            ],
        )
        if when_to_wear:
            cards["When to wear"] = when_to_wear

        # 3. Community Interest / wardrobe relation.
        relation = status.get("relation")
        if isinstance(relation, dict):
            relation_sum = safe_int(status.get("relation_sum"))
            if relation_sum <= 0:
                relation_sum = sum(safe_int(v) for v in relation.values())

            if relation_sum > 0:
                relation_rows = [
                    ("have", "Have", safe_int(relation.get("have", 0))),
                    ("had", "Had", safe_int(relation.get("had", 0))),
                    ("want", "Want", safe_int(relation.get("want", 0))),
                ]

                dominant_key, dominant_label, dominant_count = max(relation_rows, key=lambda item: item[2])
                want_count = safe_int(relation.get("want", 0))
                have_count = safe_int(relation.get("have", 0))

                cards["Community Interest"] = [
                    {
                        "label": label,
                        "display": label,
                        "value": key,
                        "pct": round((count / relation_sum) * 100.0, 2),
                        "count": str(count),
                        "source": "Fragrantica-Status",
                        "dominant": label == dominant_label,
                        "dominant_label": dominant_label,
                        "dominant_count": str(dominant_count),
                        "total_votes": relation_sum,
                        "want_ratio": round(want_count / relation_sum, 4),
                        "ownership_ratio": round(have_count / relation_sum, 4),
                    }
                    for key, label, count in relation_rows
                ]

        # 4. Rating distribution: richer than average-only summary.
        rating_distribution = distribution_card(
            card_name="Rating Distribution",
            dist_key="rating",
            sum_key="rating_sum",
            avg_key="rating_average",
            labels={
                "1": "1 Star",
                "2": "2 Stars",
                "3": "3 Stars",
                "4": "4 Stars",
                "5": "5 Stars",
            },
            source="Fragrantica-Status",
        )
        if rating_distribution:
            cards["Rating Distribution"] = rating_distribution

        return cards







    @staticmethod
    def bar_pct_from_node(node) -> float | None:
        """Read a percent value from a single bar-like node.
        Tries (in order): inline width, CSS custom properties, aria-valuenow,
        data-pct/value/percent, <progress>/<meter>. Returns float in [0, 100]
        or None when nothing matched."""
        if node is None or not hasattr(node, "get"):
            return None
        style = str(node.get("style", "") or "")
        if style:
            m = re.search(r"(?<![-\w])width\s*:\s*([\d.]+)\s*%", style, re.I)
            if m:
                try:
                    return max(0.0, min(100.0, float(m.group(1))))
                except ValueError:
                    pass
            for var in ("--w", "--width", "--progress", "--value", "--pct"):
                m = re.search(re.escape(var) + r"\s*:\s*([\d.]+)\s*%?", style, re.I)
                if m:
                    try:
                        return max(0.0, min(100.0, float(m.group(1))))
                    except ValueError:
                        pass
        valuenow = node.get("aria-valuenow")
        if valuenow not in (None, ""):
            try:
                val = float(str(valuenow).strip())
                maxv_raw = node.get("aria-valuemax")
                maxv = float(str(maxv_raw).strip()) if maxv_raw else 100.0
                if maxv > 0:
                    return max(0.0, min(100.0, val / maxv * 100.0))
            except (TypeError, ValueError):
                pass
        for attr in ("data-pct", "data-value", "data-percent", "data-percentage"):
            v = node.get(attr)
            if v in (None, ""):
                continue
            try:
                return max(0.0, min(100.0, float(str(v).strip().rstrip("%"))))
            except ValueError:
                continue
        if getattr(node, "name", None) in {"progress", "meter"}:
            try:
                val = float(str(node.get("value", "")).strip())
                maxv_raw = node.get("max", "1")
                maxv = float(str(maxv_raw).strip())
                if maxv > 0:
                    return max(0.0, min(100.0, val / maxv * 100.0))
            except (TypeError, ValueError):
                pass
        return None














    @staticmethod
    def _compact_metric_key(text: str) -> str:
        return re.sub(r"[^a-z]", "", (text or "").lower())

    @staticmethod
    def _find_performance_title_node(root, card_name: str):
        wanted = FragranticaEngine._compact_metric_key(card_name)
        if not root or not wanted:
            return None

        for tag in root.find_all(["h2", "h3", "h4", "h5", "div", "span", "b", "p"]):
            text = tag.get_text(" ", strip=True)
            if not text or len(text) > 80:
                continue
            if FragranticaEngine._compact_metric_key(text) == wanted:
                return tag
        return None

    @staticmethod
    def _node_contains(parent, child) -> bool:
        cur = child
        while cur is not None:
            if cur is parent:
                return True
            cur = getattr(cur, "parent", None)
        return False

    @staticmethod
    def _container_has_metric_label(container, card_key: str) -> bool:
        if container is None or not hasattr(container, "find_all"):
            return False
        for text_node in container.find_all(string=True):
            raw = TextSanitizer.clean(text_node)
            if not raw or len(raw) > 80:
                continue
            resolved, _ = FragranticaEngine._canonical_label(card_key, raw)
            if resolved:
                return True
        return False

    @staticmethod
    def _find_performance_card_container(root, title_node, other_title_nodes, card_key: str):
        if title_node is None:
            return None

        cur = title_node.parent
        for _ in range(10):
            if cur is None:
                return None

            contains_other = any(
                FragranticaEngine._node_contains(cur, other)
                for other in other_title_nodes
            )
            if not contains_other and FragranticaEngine._container_has_metric_label(cur, card_key):
                return cur

            if cur is root:
                break
            cur = cur.parent

        return title_node.parent

    @staticmethod
    def _text_is_count(text: str) -> bool:
        text = TextSanitizer.clean(text)
        return bool(re.fullmatch(r"[\d,.]+[kKmM]?", text or ""))

    @staticmethod
    def _count_to_float(count: Any) -> float:
        text = TextSanitizer.clean(count).replace(",", "").lower()
        if not text:
            return 0.0
        multiplier = 1.0
        if text.endswith("k"):
            multiplier = 1_000.0
            text = text[:-1]
        elif text.endswith("m"):
            multiplier = 1_000_000.0
            text = text[:-1]
        try:
            return float(text) * multiplier
        except ValueError:
            return 0.0

    @staticmethod
    def _extract_bar_pct_from_row(row) -> float | None:
        if row is None or not hasattr(row, "find_all"):
            return None

        bar_re = re.compile(r"width\s*:\s*[\d.]+%", re.I)
        signals = [
            b for b in row.find_all("div", style=bar_re)
            if not b.find("div", style=bar_re)
        ]

        if hasattr(row, "select"):
            for sel in ("[aria-valuenow]", "progress", "meter", "[data-pct]", "[data-value]", "[data-percent]"):
                signals.extend(row.select(sel))

        seen_ids: set[int] = set()
        for signal in signals:
            if id(signal) in seen_ids:
                continue
            seen_ids.add(id(signal))
            pct = FragranticaEngine.bar_pct_from_node(signal)
            if pct is not None:
                return pct

        return None

    @staticmethod
    def _extract_count_for_label(texts: list[str], card_key: str, canonical: str) -> str:
        label_index = -1

        for idx, text in enumerate(texts):
            resolved, _ = FragranticaEngine._canonical_label(card_key, text)
            if resolved == canonical:
                label_index = idx

                for group in FragranticaEngine.KNOWN_METRIC_LABELS.get(card_key, []):
                    if group[0] != canonical:
                        continue
                    for alias in group:
                        m = re.search(
                            rf"\b{re.escape(alias)}\b\s*([\d,.]+[kKmM]?)",
                            text,
                            re.I,
                        )
                        if m:
                            return m.group(1)
                break

        if label_index >= 0:
            for text in texts[label_index + 1:]:
                if FragranticaEngine._text_is_count(text):
                    return text

        for text in texts:
            if FragranticaEngine._text_is_count(text):
                return text

        return ""

    @staticmethod
    def _read_performance_row_from_label(label_node, container, card_key: str, canonical: str) -> dict[str, Any] | None:
        cur = label_node.parent if hasattr(label_node, "parent") else None

        for _ in range(8):
            if cur is None or not hasattr(cur, "stripped_strings"):
                return None

            texts = [
                TextSanitizer.clean(s)
                for s in cur.stripped_strings
                if TextSanitizer.clean(s)
            ]

            if texts and not any(
                any(bad in t.lower() for bad in FragranticaEngine.BAD_UI_TEXTS)
                for t in texts
            ):
                labels_found: list[str] = []
                for text in texts:
                    resolved, _ = FragranticaEngine._canonical_label(card_key, text)
                    if resolved and resolved not in labels_found:
                        labels_found.append(resolved)

                if labels_found == [canonical]:
                    count = FragranticaEngine._extract_count_for_label(texts, card_key, canonical)
                    pct = FragranticaEngine._extract_bar_pct_from_row(cur)

                    if count or pct is not None:
                        display = canonical
                        if card_key in ("price value", "value"):
                            display = FragranticaEngine.PRICE_VALUE_DISPLAY.get(canonical, canonical)

                        return {
                            "label": canonical,
                            "display": display,
                            "pct": float(pct) if pct is not None else 0.0,
                            "count": count,
                            "source": "Fragrantica",
                        }

            if cur is container:
                break
            cur = cur.parent

        return None

    @staticmethod
    def _find_performance_label_node(container, card_key: str, canonical: str, group: list[str], consumed_ids: set[int]):
        if container is None or not hasattr(container, "find_all"):
            return None

        for alias in group:
            alias_norm = TextSanitizer.normalize_identity(alias)
            if not alias_norm:
                continue

            for text_node in container.find_all(string=True):
                if id(text_node) in consumed_ids:
                    continue

                raw = TextSanitizer.clean(text_node)
                if not raw or len(raw) > 80:
                    continue

                norm = TextSanitizer.normalize_identity(raw)
                if not norm:
                    continue

                if not (
                    norm == alias_norm
                    or norm.startswith(alias_norm + " ")
                    or (" " + alias_norm + " ") in norm
                ):
                    continue

                resolved, _ = FragranticaEngine._canonical_label(card_key, raw)
                if resolved == canonical:
                    return text_node

        return None

    @staticmethod
    def _finalize_performance_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []

        total = sum(FragranticaEngine._count_to_float(row.get("count", "")) for row in rows)
        dominant = max(
            rows,
            key=lambda row: FragranticaEngine._count_to_float(row.get("count", "")),
            default=None,
        )

        dominant_label = dominant.get("label", "") if dominant else ""
        dominant_count = dominant.get("count", "") if dominant else ""

        for row in rows:
            count_value = FragranticaEngine._count_to_float(row.get("count", ""))
            if not row.get("pct") and total > 0:
                row["pct"] = round((count_value / total) * 100.0, 2)

            row["dominant"] = bool(row.get("label") == dominant_label)
            row["dominant_label"] = dominant_label
            row["dominant_count"] = dominant_count
            row["total_votes"] = int(total) if float(total).is_integer() else total

        return rows

    # Strict dominant-label sets used by the visible-label fallback. Order
    # mirrors the page language map so the first match wins when iterating.
    _PERFORMANCE_DOMINANT_LABELS: dict[str, list[str]] = {
        "Longevity": ["Very Weak", "Weak", "Moderate", "Long Lasting", "Eternal"],
        "Sillage": ["Intimate", "Moderate", "Strong", "Enormous"],
        "Price Value": ["Way Overpriced", "Overpriced", "Ok", "Good Value", "Great Value"],
    }

    @staticmethod
    def _bounded_local_container(title_node, other_title_nodes, max_hops: int = 8):
        """Walk up from title_node until just before an ancestor includes any
        other performance title node. Returns the tightest ancestor that
        contains title_node but not the others. None if title_node is None."""
        if title_node is None:
            return None
        bounded = None
        cur = title_node
        for _ in range(max_hops):
            cur = getattr(cur, "parent", None)
            if cur is None:
                break
            contains_other = any(
                FragranticaEngine._node_contains(cur, other)
                for other in other_title_nodes
            )
            if contains_other:
                break
            bounded = cur
        return bounded

    @staticmethod
    def _extract_visible_dominant_performance_labels(root) -> dict[str, list[dict[str, Any]]]:
        """Last-resort fallback: when richer row extraction yields nothing for a
        performance card, look inside the local container around the card's
        title for exactly one known label. Emit a single dominant row with no
        count/pct. Never scans the whole page; never fabricates distributions."""
        out: dict[str, list[dict[str, Any]]] = {}
        if root is None:
            return out

        root_text = TextSanitizer.clean(root.get_text(" ", strip=True))
        root_norm = TextSanitizer.normalize_identity(root_text)
        if not root_norm:
            return out

        title_nodes: dict[str, Any] = {}
        for card_name in FragranticaEngine.PERFORMANCE_CARD_NAMES.values():
            node = FragranticaEngine._find_performance_title_node(root, card_name)
            if node is not None:
                title_nodes[card_name] = node

        section_has = {
            "Longevity": "longevity" in root_norm,
            "Sillage": "sillage" in root_norm,
            "Price Value": "price value" in root_norm or "pricevalue" in root_norm,
        }

        for card_name, title_node in title_nodes.items():
            if not section_has.get(card_name):
                continue

            labels_for_card = FragranticaEngine._PERFORMANCE_DOMINANT_LABELS.get(card_name, [])
            if not labels_for_card:
                continue

            other_titles = [n for c, n in title_nodes.items() if c != card_name]
            container = FragranticaEngine._bounded_local_container(title_node, other_titles)
            if container is None:
                continue

            local_text = TextSanitizer.clean(container.get_text(" ", strip=True))
            local_norm = TextSanitizer.normalize_identity(local_text)
            if not local_norm:
                continue

            # Whole-word match, longest alias first so "Long Lasting" wins over
            # "Lasting" and "Very Weak" wins over "Weak". Track consumed spans
            # so a shorter alias can't double-count under a longer one.
            sorted_labels = sorted(labels_for_card, key=lambda s: -len(s))
            consumed: list[tuple[int, int]] = []
            found: list[str] = []
            for label in sorted_labels:
                label_norm = TextSanitizer.normalize_identity(label)
                if not label_norm:
                    continue
                pattern = re.compile(r"(?:^|\s)" + re.escape(label_norm) + r"(?:\s|$)")
                m = pattern.search(local_norm)
                if not m:
                    continue
                s, e = m.span()
                if any(not (e <= cs or s >= ce) for cs, ce in consumed):
                    continue
                consumed.append((s, e))
                found.append(label)

            if len(found) != 1:
                continue

            canonical = found[0]
            display = canonical
            card_key = FragranticaEngine._card_key_for_title(card_name)
            if card_key in ("price value", "value"):
                display = FragranticaEngine.PRICE_VALUE_DISPLAY.get(canonical, canonical)

            out[card_name] = [{
                "label": canonical,
                "display": display,
                "pct": 0.0,
                "count": "",
                "source": "Fragrantica",
                "dominant": True,
            }]

        return out

    @staticmethod
    def extract_performance_metrics(soup: BeautifulSoup) -> dict[str, list[dict[str, Any]]]:
        cards: dict[str, list[dict[str, Any]]] = {}
        root = soup.select_one("#performance") if soup else None

        if root is None:
            return cards

        title_nodes: dict[str, Any] = {}
        for compact_name, card_name in FragranticaEngine.PERFORMANCE_CARD_NAMES.items():
            node = FragranticaEngine._find_performance_title_node(root, card_name)
            if node is not None:
                title_nodes[card_name] = node

        for card_name, title_node in title_nodes.items():
            card_key = FragranticaEngine._card_key_for_title(card_name)
            if not card_key:
                continue

            other_titles = [
                node for name, node in title_nodes.items()
                if name != card_name
            ]
            container = FragranticaEngine._find_performance_card_container(
                root,
                title_node,
                other_titles,
                card_key,
            )
            if container is None:
                continue

            rows: list[dict[str, Any]] = []
            consumed_ids: set[int] = set()
            seen_labels: set[str] = set()

            for group in FragranticaEngine.KNOWN_METRIC_LABELS.get(card_key, []):
                canonical = group[0]
                if canonical in seen_labels:
                    continue

                label_node = FragranticaEngine._find_performance_label_node(
                    container,
                    card_key,
                    canonical,
                    group,
                    consumed_ids,
                )
                if label_node is None:
                    continue

                consumed_ids.add(id(label_node))

                row = FragranticaEngine._read_performance_row_from_label(
                    label_node,
                    container,
                    card_key,
                    canonical,
                )
                if not row:
                    continue

                rows.append(row)
                seen_labels.add(canonical)

            finalized = FragranticaEngine._finalize_performance_rows(rows)
            if finalized:
                cards[card_name] = finalized

        # Dominant-label fallback for any title visible in #performance whose
        # richer row extraction returned nothing. One row, no count/pct, no
        # fabricated distribution.
        missing = [c for c in title_nodes if c not in cards]
        if missing:
            fallback = FragranticaEngine._extract_visible_dominant_performance_labels(root)
            for card_name in missing:
                rows_fb = fallback.get(card_name)
                if rows_fb:
                    cards[card_name] = rows_fb

        return cards























    @staticmethod
    def find_tightest_card_container(label_node, other_label_nodes, max_hops: int = 8):
        """Climb from label_node until reaching an ancestor that contains a bar
        signal but does NOT contain any other card's title node as a descendant.
        Returns None if no such ancestor exists within max_hops."""
        if label_node is None:
            return None
        bar_re = re.compile(r"width\s*:\s*[\d.]+%", re.I)
        cur = label_node.parent
        for _ in range(max_hops):
            if cur is None:
                return None
            has_bars = False
            if hasattr(cur, "find") and cur.find("div", style=bar_re) is not None:
                has_bars = True
            elif hasattr(cur, "select_one") and cur.select_one(
                "[aria-valuenow], progress, meter, [data-pct], [data-value], [data-percent]"
            ) is not None:
                has_bars = True
            if has_bars:
                contains_other = False
                for other in other_label_nodes:
                    anc = other
                    while anc is not None:
                        if anc is cur:
                            contains_other = True
                            break
                        anc = anc.parent
                    if contains_other:
                        break
                if not contains_other:
                    return cur
            cur = cur.parent
        return None





    






















    @staticmethod
    def extract_metrics_from_json(soup: BeautifulSoup) -> dict[str, list[dict[str, Any]]]:
        """Pass 1: pull metric distributions from any structured JSON embedded in
        the page (JSON-LD, __NEXT_DATA__, inline JS assignments). Best-effort —
        returns {} when no shape is recognised, and the merger falls back to DOM."""
        cards: dict[str, list[dict[str, Any]]] = {}
        if soup is None:
            return cards
        payloads: list[Any] = []
        inline_patterns = (
            r"voteData\s*=\s*(\{.*?\})\s*[;,\n]",
            r"__INITIAL_STATE__\s*=\s*(\{.*?\})\s*[;,\n]",
            r"__APOLLO_STATE__\s*=\s*(\{.*?\})\s*[;,\n]",
            r"window\.__data\s*=\s*(\{.*?\})\s*[;,\n]",
        )
        for script in soup.find_all("script"):
            stype = (script.get("type") or "").lower()
            sid = (script.get("id") or "").lower()
            raw = script.string or script.get_text() or ""
            if not raw:
                continue
            if stype == "application/ld+json" or sid == "__next_data__":
                try:
                    payloads.append(json.loads(raw.strip()))
                except Exception:
                    pass
            for pattern in inline_patterns:
                for m in re.finditer(pattern, raw, re.DOTALL):
                    chunk = FragranticaEngine._balanced_brace_slice(raw, m.start(1))
                    if not chunk:
                        continue
                    try:
                        payloads.append(json.loads(chunk))
                    except Exception:
                        pass

        metric_key_to_card = {
            "longevity": "Longevity",
            "sillage": "Sillage",
            "projection": "Projection",
            "gender": "Gender",
            "value": "Price Value",
            "pricevalue": "Price Value",
            "season": "When to wear",
            "daynight": "When to wear",
            "whentowear": "When to wear",
            "wear": "When to wear",
        }

        def push(card_name: str, label_text: str, pct: Any, count: Any = "") -> None:
            try:
                pct_f = float(str(pct).strip().rstrip("%"))
            except (TypeError, ValueError):
                return
            if not (0.0 <= pct_f <= 100.0):
                return
            card_key = FragranticaEngine._card_key_for_title(card_name)
            canon, _ = FragranticaEngine._canonical_label(card_key, str(label_text or ""))
            label = canon or TextSanitizer.clean(label_text).title()
            if not label:
                return
            entry: dict[str, Any] = {
                "label": label,
                "pct": pct_f,
                "count": TextSanitizer.clean(count),
                "source": "Fragrantica-JSON",
            }
            if card_key in ("price value", "value"):
                entry["display"] = FragranticaEngine.PRICE_VALUE_DISPLAY.get(label, label)
            cards.setdefault(card_name, []).append(entry)

        def walk(obj):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    key_norm = re.sub(r"[^a-z]", "", str(k).lower())
                    if key_norm in metric_key_to_card and isinstance(v, (dict, list)):
                        card_name = metric_key_to_card[key_norm]
                        if isinstance(v, dict):
                            numeric = [val for val in v.values() if isinstance(val, (int, float))]
                            if numeric and len(numeric) == len(v):
                                for label_text, pct in v.items():
                                    push(card_name, label_text, pct)
                            elif "label" in v:
                                push(card_name, v.get("label"), v.get("pct") or v.get("percent") or v.get("value"), v.get("count") or v.get("votes"))
                        elif isinstance(v, list):
                            for item in v:
                                if isinstance(item, dict) and "label" in item:
                                    push(card_name, item.get("label"), item.get("pct") or item.get("percent") or item.get("value"), item.get("count") or item.get("votes"))
                    walk(v)
            elif isinstance(obj, list):
                for item in obj:
                    walk(item)

        for payload in payloads:
            try:
                walk(payload)
            except Exception:
                continue
        return cards

    @staticmethod
    def _balanced_brace_slice(text: str, start_idx: int) -> str:
        if start_idx >= len(text) or text[start_idx] != "{":
            return ""
        depth = 0
        in_str = False
        escape = False
        for i in range(start_idx, len(text)):
            ch = text[i]
            if in_str:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == '"':
                    in_str = False
                continue
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return text[start_idx : i + 1]
        return ""

    @staticmethod
    def _find_row_signal(start_node, max_hops: int = 5) -> tuple[float | None, str]:
        """Walk up from start_node up to max_hops. The level where exactly one
        bar-like signal exists is treated as the row level and returns its pct
        plus any tabular count text found in that row."""
        bar_re = re.compile(r"width\s*:\s*[\d.]+%", re.I)
        cur = start_node.parent if hasattr(start_node, "parent") else None
        for _ in range(max_hops):
            if cur is None:
                return None, ""
            if not hasattr(cur, "find_all"):
                cur = getattr(cur, "parent", None)
                continue
            all_bars = cur.find_all("div", style=bar_re)
            bars = [b for b in all_bars if not b.find("div", style=bar_re)]
            non_div = []
            if hasattr(cur, "select"):
                for sel in ("[aria-valuenow]", "progress", "meter", "[data-pct]", "[data-value]", "[data-percent]"):
                    non_div.extend(cur.select(sel))
            signals = bars + non_div
            if len(signals) == 1:
                pct = FragranticaEngine.bar_pct_from_node(signals[0])
                count = ""
                for s in cur.stripped_strings:
                    text = TextSanitizer.clean(s)
                    if not text or text == "0":
                        continue
                    if re.fullmatch(r"[\d,.]+[kKmM]?", text):
                        count = text
                        break
                return pct, count
            if len(signals) > 1:
                return None, ""
            cur = cur.parent
        return None, ""

    @staticmethod
    def extract_generic_rating_card(container, card_name: str) -> list[dict[str, Any]]:
        """Pass 2: vocabulary-bound DOM walk. For each alias-group in the card's
        vocabulary, find a text node that matches the alias and resolves back to
        the group's canonical label, then read the nearest bar signal in the
        same row."""
        if container is None:
            return []
        card_key = FragranticaEngine._card_key_for_title(card_name)
        groups = FragranticaEngine.KNOWN_METRIC_LABELS.get(card_key, [])
        title_norm = TextSanitizer.normalize_identity(card_name)
        metrics: list[dict[str, Any]] = []
        seen_canonical: set[str] = set()
        consumed_text_nodes: set[int] = set()

        for group in groups:
            canonical = group[0]
            if canonical in seen_canonical:
                continue
            match = FragranticaEngine._match_group_in_container(
                container, card_key, canonical, group, title_norm, consumed_text_nodes
            )
            if match is None:
                continue
            pct, count, text_node = match
            consumed_text_nodes.add(id(text_node))
            display = canonical
            if card_key in ("price value", "value"):
                display = FragranticaEngine.PRICE_VALUE_DISPLAY.get(canonical, canonical)
            metrics.append({
                "label": canonical,
                "display": display,
                "pct": float(pct),
                "count": count,
                "source": "Fragrantica",
            })
            seen_canonical.add(canonical)

        if metrics:
            return metrics

        # Vocabulary failed entirely: fall back to row-text labelling (no
        # positional indexing — extra/missing bars used to shift every label).
        bar_re = re.compile(r"width\s*:\s*[\d.]+%", re.I)
        bars = [b for b in container.find_all("div", style=bar_re) if not b.find("div", style=bar_re)]
        for bar in bars:
            pct = FragranticaEngine.bar_pct_from_node(bar)
            if pct is None:
                continue
            row = bar.parent
            label = ""
            count = ""
            for _ in range(5):
                if row is None:
                    break
                texts = [TextSanitizer.clean(s) for s in row.stripped_strings if s]
                label_parts: list[str] = []
                for t in texts:
                    t_low = t.lower()
                    # Globally block screen reader/voting texts from infecting fallbacks
                    if any(bad in t_low for bad in FragranticaEngine.BAD_UI_TEXTS):
                        continue
                    if re.fullmatch(r"[\d,.]+[kKmM]?", t):
                        count = t
                    elif TextSanitizer.normalize_identity(t) != title_norm and not re.match(r"^[\d.]+$", t):
                        label_parts.append(t)
                if label_parts:
                    label = " ".join(label_parts).strip()
                    break
                row = row.parent
            if not label:
                continue
            
            resolved, _ = FragranticaEngine._canonical_label(card_key, label)
            # VOCABULARY STRICT-ENFORCEMENT FIX:
            # If the fallback label is NOT an explicitly known canonical alias for this card
            # (like 'Winter' or 'Day' for 'When to wear'), skip it! This completely stops
            # the DOM from grabbing adjacent arbitrary chart data if it crawls too high.
            if not resolved:
                continue
                
            canonical = resolved
            if canonical in seen_canonical:
                continue
                
            display = canonical
            if card_key in ("price value", "value"):
                display = FragranticaEngine.PRICE_VALUE_DISPLAY.get(canonical, canonical)
            metrics.append({
                "label": canonical,
                "display": display,
                "pct": float(pct),
                "count": count,
                "source": "Fragrantica",
            })
            seen_canonical.add(canonical)
        return metrics

    @staticmethod
    def _match_group_in_container(container, card_key, canonical, group, title_norm, consumed_text_nodes):
        for alias in group:
            alias_norm = TextSanitizer.normalize_identity(alias)
            if not alias_norm:
                continue
            for text_node in container.find_all(string=True):
                if id(text_node) in consumed_text_nodes:
                    continue
                raw = TextSanitizer.clean(text_node)
                if not raw or len(raw) > 60:
                    continue
                norm = TextSanitizer.normalize_identity(raw)
                if not norm or norm == title_norm:
                    continue
                if not (
                    norm == alias_norm
                    or norm.startswith(alias_norm + " ")
                    or norm.endswith(" " + alias_norm)
                    or (" " + alias_norm + " ") in norm
                ):
                    continue
                # Disambiguate so "Female" doesn't claim "More Female".
                resolved, _ = FragranticaEngine._canonical_label(card_key, raw)
                if resolved and resolved != canonical:
                    continue
                pct, count = FragranticaEngine._find_row_signal(text_node)
                if pct is None:
                    continue
                return pct, count, text_node
        return None

    @staticmethod
    def extract_metrics_from_text(container, card_key: str) -> list[dict[str, Any]]:
        """Pass 3: regex-only fallback over the container's flattened text. Used
        when DOM signals are missing entirely (e.g. plain text rendering)."""
        if container is None or card_key not in FragranticaEngine.KNOWN_METRIC_LABELS:
            return []
        text = container.get_text(" ", strip=True) if hasattr(container, "get_text") else str(container)
        text = TextSanitizer.clean(text)
        if not text:
            return []
        out: list[dict[str, Any]] = []
        seen: set[str] = set()
        for group in FragranticaEngine.KNOWN_METRIC_LABELS[card_key]:
            canonical = group[0]
            if canonical in seen:
                continue
            for alias in group:
                esc = re.escape(alias)
                pct: float | None = None
                count = ""
                for pattern in (
                    rf"{esc}\s*\(\s*([\d.]+)\s*%\s*\)\s*([\d,.]+[kKmM]?)?",
                    rf"{esc}\s*[:\-–]\s*([\d.]+)\s*%\s*([\d,.]+[kKmM]?)?",
                    rf"{esc}\s+([\d.]+)\s*%\s+([\d,.]+[kKmM]?)?",
                ):
                    m = re.search(pattern, text, re.I)
                    if not m:
                        continue
                    try:
                        pct = float(m.group(1))
                    except (TypeError, ValueError):
                        continue
                    count = (m.group(2) or "").strip() if m.lastindex and m.lastindex >= 2 else ""
                    break
                if pct is None:
                    continue
                if not (0.0 <= pct <= 100.0):
                    continue
                entry: dict[str, Any] = {
                    "label": canonical,
                    "pct": float(pct),
                    "count": count,
                    "source": "Fragrantica-Text",
                }
                if card_key in ("price value", "value"):
                    entry["display"] = FragranticaEngine.PRICE_VALUE_DISPLAY.get(canonical, canonical)
                out.append(entry)
                seen.add(canonical)
                break
        return out

    @staticmethod
    def merge_metric_passes(passes: list[dict[str, list[dict[str, Any]]]]) -> dict[str, list[dict[str, Any]]]:
        """Merge passes in priority order (earliest pass wins per label).
        Drops invalid pct values and empty cards."""
        merged: dict[str, list[dict[str, Any]]] = {}
        index: dict[str, dict[str, dict[str, Any]]] = {}
        for pass_data in passes:
            if not pass_data:
                continue
            for card, metrics in pass_data.items():
                target = merged.setdefault(card, [])
                seen = index.setdefault(card, {})
                for metric in metrics:
                    try:
                        pct = float(metric.get("pct", 0) or 0)
                    except (TypeError, ValueError):
                        continue
                    if not (0.0 <= pct <= 100.0):
                        continue
                    label_norm = TextSanitizer.normalize_identity(str(metric.get("label", "")))
                    if not label_norm:
                        continue
                    if label_norm in seen:
                        continue  # earlier (higher-priority) pass wins
                    target.append(metric)
                    seen[label_norm] = metric
        return {card: items for card, items in merged.items() if items}
        
    @staticmethod
    def extract_notes_after_label(soup: BeautifulSoup, label: str) -> list[str]:
        # Strip all whitespace/punctuation to create an indestructible match key
        # e.g., "Top Notes" -> "topnotes"
        target_clean = re.sub(r"[^a-z]", "", label.lower())
        
        label_node = None
        # Safest text search: iterate targets, clean text, and compare
        for tag in soup.find_all(["h2", "h3", "h4", "h5", "span", "div", "b", "strong", "p"]):
            text = tag.get_text(strip=True)
            # Ignore massive text blocks to prevent false positives
            if not text or len(text) > 60:
                continue
            
            text_clean = re.sub(r"[^a-z]", "", text.lower())
            if target_clean in text_clean:
                label_node = tag
                break
                
        # Fallback to images if text headers are missing
        if not label_node:
            for img in soup.find_all("img"):
                alt = re.sub(r"[^a-z]", "", img.get("alt", "").lower())
                title = re.sub(r"[^a-z]", "", img.get("title", "").lower())
                if target_clean in alt or target_clean in title:
                    label_node = img
                    break
                    
        if not label_node:
            return []

        # Target the Vue container class (e.g., pyramid-level-1-container)
        # Using "pyramid-level" acts as a wildcard to catch injected numbers
        container = label_node.find_next("div", class_=re.compile(r"pyramid-level", re.I))
        
        # If find_next jumps out of scope, traverse the direct DOM siblings
        if not container:
            parent = label_node.parent
            for _ in range(4):
                if not parent:
                    break
                for sibling in parent.find_next_siblings("div"):
                    if sibling.select("a.pyramid-note-link") or sibling.find("div", class_=re.compile(r"pyramid-level", re.I)):
                        container = sibling
                        break
                if container:
                    break
                parent = parent.parent

        notes = []
        
        # Pass 1: Extract from the identified pyramid container
        if container:
            links = container.select("a.pyramid-note-link")
            if not links:
                links = container.find_all("a")

            for link in links:
                label_span = link.find("span", class_=re.compile(r"pyramid-note-label", re.I))
                text = safe_get_text(label_span) if label_span else safe_get_text(link)
                if text:
                    notes.append(text)
        
        # Pass 2: Aggressive DOM walk for non-standard/legacy layouts
        if not notes:
            cur = label_node.next_sibling
            while cur:
                name = getattr(cur, "name", None)
                # Break condition: We hit the next logical section header
                if name in {"h2", "h3", "h4"} or (name in {"div", "b", "span", "strong"} and 0 < len(cur.get_text(strip=True)) < 40 and ("notes" in cur.get_text(strip=True).lower() or "base" in cur.get_text(strip=True).lower())):
                    text_clean = re.sub(r"[^a-z]", "", cur.get_text(strip=True).lower())
                    if target_clean not in text_clean:
                        break 
                
                if hasattr(cur, "find_all"):
                    for img in cur.find_all("img"):
                        alt = img.get("alt", "").strip()
                        parent_text = safe_get_text(img.parent) if getattr(img.parent, "name", None) == "a" else ""
                        
                        if alt and len(alt) < 40 and "search" not in alt.lower():
                            notes.append(alt)
                        elif parent_text and len(parent_text) < 40:
                            notes.append(parent_text)
                            
                cur = cur.next_sibling

        return unique_clean(notes)

    @staticmethod
    def extract_flat_notes(soup: BeautifulSoup) -> list[str]:
        notes = []

        # 1. Fast path: globally select the exact note links (Vue Layout)
        links = soup.select("a.pyramid-note-link")
        if links:
            for link in links:
                label_span = link.find("span", class_=re.compile(r"pyramid-note-label", re.I))
                text = safe_get_text(label_span) if label_span else safe_get_text(link)
                if text:
                    notes.append(text)
            if notes:
                return unique_clean(notes)

        # 2. The Vue.js Script Bypass (Catches client-rendered pages like Hermès Paddock)
        # We run this BEFORE loose DOM scans to prevent grabbing unrelated UI text.
        for script in soup.find_all("script"):
            content = str(script.string or script.get_text() or "")
            # Normalize escaped slashes (\/ or \u002F) to standard forward slashes
            content = content.replace("\\/", "/").replace("\\u002f", "/").replace("\\u002F", "/")
            
            # Snipe the note URLs directly out of the raw Nuxt/Vue JSON state blob
            for m in re.finditer(r'/(?:notes|perfume-ingredients)/([^/"\'<>]+)-\d+\.html', content, re.I):
                # Reverse-engineer the display name from the URL slug (e.g. "Carrot-Seeds" -> "Carrot Seeds")
                note_name = m.group(1).replace("-", " ").title()
                if note_name and len(note_name) < 40 and "Search" not in note_name:
                    notes.append(note_name)
        if notes:
            return unique_clean(notes)
            
        # 3. The Regex Net (Bypass UI wrappers, target the DB links directly in the DOM)
        for a in soup.find_all("a", href=re.compile(r"/(?:notes|perfume-ingredients)/", re.I)):
            text = safe_get_text(a)
            if not text:
                img = a.find("img")
                if img:
                    text = img.get("alt", "")
            
            # Clean up hidden screen-reader text
            text = re.sub(r"(?i)\s+odour profile.*", "", text).strip()
            
            if text and len(text) < 40 and "search" not in text.lower() and "news" not in text.lower():
                notes.append(text)
        if notes:
            return unique_clean(notes)

        # 4. Legacy fallback: general container scanning
        for container in soup.find_all("div", class_=re.compile(r"pyramid-level|notes-box", re.I)):
            for a in container.find_all("a"):
                text = safe_get_text(a)
                if text and len(text) < 40 and "search" not in text.lower():
                    notes.append(text)
        if notes:
            return unique_clean(notes)

        # 5. Static images with note pathways (Final Safety Net)
        for img in soup.find_all("img", src=re.compile(r"/notes/", re.I)):
            alt = img.get("alt", "").strip()
            if alt and len(alt) < 40:
                notes.append(alt)

        return unique_clean(notes)
        
    @staticmethod
    def extract_rating_summary(soup: BeautifulSoup) -> dict[str, Any] | None:
        text = safe_get_text(soup)
        match = re.search(r"Perfume rating\s*([0-5](?:\.\d+)?)\s*out of\s*5\s*with\s*([\d,]+)\s*votes", text, re.I)
        if not match:
            return None
        return {"label": "Rating", "value": f"{match.group(1)}/5", "count": f"{match.group(2)} votes", "source": "Fragrantica"}
        
    @staticmethod
    def extract_people_say(soup: BeautifulSoup, existing: list[str]) -> list[str]:
        header = soup.find(lambda tag: getattr(tag, "name", None) in {"h2", "h3", "h4"} and "what people say" in tag.get_text(" ", strip=True).lower())
        if not header:
            return []
        container = header.find_next_sibling("div", class_=re.compile(r"grid", re.I)) or header.parent
        out: list[str] = []
        for string in container.stripped_strings if container else []:
            text = TextSanitizer.clean(string)
            low = text.lower()
            if len(text) > 15 and "what people say" not in low and text not in existing and text not in out:
                out.append(text)
        return out
        
    @staticmethod
    def extract_reviews(soup: BeautifulSoup, limit: int = 8) -> list[Review]:
        reviews: list[Review] = []
        block_re = re.compile(r"review|comment|user-review|fragrantica-review", re.I)
        bad = re.compile(r"perfume rating|main accords|similar perfumes|add your review|be the first|login|sign up", re.I)
        for node in soup.find_all(["article", "section", "div"], class_=block_re):
            text = safe_get_text(node, separator=" | ")
            text = re.sub(r"(?:\s*\|\s*)+", " | ", text).strip(" |")
            if 70 <= len(text) <= 1600 and not bad.search(text):
                reviews.append(Review(text=text, source="Fragrantica"))
            if len(reviews) >= limit:
                break
        return dedupe_reviews(reviews)
        
    @staticmethod
    def extract_rating_card_metrics(card, card_name: str) -> list[dict[str, Any]]:
        """Legacy tw-rating-card layout. Each column carries one label + bar.
        Vocabulary-bound: we resolve the column's text to a canonical label and
        only fall back to the parsed text when vocabulary doesn't match (no
        positional indexing)."""
        metrics: list[dict[str, Any]] = []
        columns = card.find_all("div", class_=re.compile(r"flex-col"))
        card_key = FragranticaEngine._card_key_for_title(card_name)
        seen_canonical: set[str] = set()

        for col in columns:
            label_parts: list[str] = []
            count = ""
            for span in col.find_all("span"):
                classes = span.get("class", [])
                classes = classes if isinstance(classes, list) else [classes]
                text = safe_get_text(span)
                if not text:
                    continue
                # Block screen reader / voting texts from bleeding into parsed legacy tags
                t_low = text.lower()
                if any(bad in t_low for bad in FragranticaEngine.BAD_UI_TEXTS):
                    continue
                is_tabular = "tabular-nums" in classes
                is_number = bool(re.fullmatch(r"[\d,.]+[kKmM]?", text))
                if is_tabular:
                    count = text
                elif not is_number:
                    label_parts.append(text)

            parsed_label = TextSanitizer.clean(" ".join(label_parts))
            resolved, _ = FragranticaEngine._canonical_label(card_key, parsed_label)
            
            # VOCABULARY STRICT-ENFORCEMENT FIX
            if not resolved:
                continue
                
            label = resolved
            pct = 0.0
            bar = col.find("div", style=re.compile(r"width\s*:", re.I))
            if bar:
                pct_val = FragranticaEngine.bar_pct_from_node(bar)
                if pct_val is not None:
                    pct = pct_val
            if pct == 0.0:
                # Try non-style bar signals (aria-valuenow / progress / meter)
                if hasattr(col, "select_one"):
                    alt = col.select_one("[aria-valuenow], progress, meter, [data-pct], [data-value], [data-percent]")
                    if alt is not None:
                        pct_val = FragranticaEngine.bar_pct_from_node(alt)
                        if pct_val is not None:
                            pct = pct_val
            if pct == 0.0 and not count:
                continue
            if label in seen_canonical:
                continue
            metric: dict[str, Any] = {
                "label": label,
                "pct": pct,
                "count": count,
                "source": "Fragrantica",
            }
            if card_key in ("price value", "value"):
                metric["display"] = FragranticaEngine.PRICE_VALUE_DISPLAY.get(label, label)
            metrics.append(metric)
            seen_canonical.add(label)
        return metrics

class Orchestrator:
    NATIVE_ACCEPT = 0.74
    CATALOG_ACCEPT = 0.76
    RELATED_ACCEPT = 0.78
    
    @staticmethod
    def match_and_merge(
        bn_results: list[UnifiedFragrance],
        frag_results: list[UnifiedFragrance],
    ) -> list[UnifiedFragrance]:
        if not bn_results:
            return frag_results
        merged: list[UnifiedFragrance] = []
        used_frag: set[int] = set()
        for bn in bn_results:
            best_index = None
            best_score = 0.0
            for idx, frag in enumerate(frag_results):
                if idx in used_frag:
                    continue
                score = Orchestrator.identity_score(bn, frag)
                if score > best_score:
                    best_score = score
                    best_index = idx
            if best_index is not None and best_score >= Orchestrator.NATIVE_ACCEPT:
                match = frag_results[best_index]
                bn.frag_url = match.frag_url
                bn.resolver_source = match.resolver_source or "native_match"
                bn.resolver_score = best_score
                if not bn.year and match.year:
                    bn.year = match.year
                if bn.brand.lower() == "unknown" and match.brand:
                    bn.brand = match.brand
                used_frag.add(best_index)
            merged.append(bn)
        return merged
        
    @staticmethod
    def identity_score(a: UnifiedFragrance | CatalogItem, b: UnifiedFragrance | CatalogItem) -> float:
        a_brand = TextSanitizer.clean(getattr(a, "brand", ""))
        b_brand = TextSanitizer.clean(getattr(b, "brand", ""))
        
        # Hard Gate: Ensure brand compatibility
        brand_ok = (
            not a_brand or not b_brand 
            or a_brand.lower() == "unknown" or b_brand.lower() == "unknown"
            or IdentityTools.compatible_brand(a_brand, b_brand)
        )
        if not brand_ok:
            return 0.0
            
        a_name_raw = TextSanitizer.clean(getattr(a, "name", ""))
        b_name_raw = TextSanitizer.clean(getattr(b, "name", ""))
        
        a_name = IdentityTools.name_tokens(a_name_raw, getattr(a, "brand", ""))
        b_name = IdentityTools.name_tokens(b_name_raw, getattr(b, "brand", ""))
        
        score = 0.0
        if not a_name or not b_name:
            ratio = IdentityTools.seq_ratio(a_name_raw, b_name_raw)
            if ratio >= 0.80:
                score = ratio
            else:
                return 0.0
        else:
            jaccard, coverage = IdentityTools.token_overlap(a_name, b_name)
            exact_name = a_name == b_name
            
            if exact_name:
                score = 0.96
            elif coverage >= 0.92:
                score = 0.90
            elif jaccard >= 0.75:
                score = 0.84
            elif jaccard >= 0.55 and coverage >= 0.70:
                score = 0.74
            else:
                return 0.0  # Hard Gate: Names do not strongly match

        a_year = TextSanitizer.clean(getattr(a, "year", ""))
        b_year = TextSanitizer.clean(getattr(b, "year", ""))
        
        if a_year and b_year:
            if a_year == b_year:
                score += 0.04
            else:
                try:
                    delta = abs(int(a_year) - int(b_year))
                    if delta > 1:
                        return 0.0 # Hard Gate: Years explicitly conflict beyond DB wobble
                    else:
                        score -= 0.15 # Minor penalty for 1-year wobble
                except ValueError:
                    return 0.0

        return max(0.0, min(score, 1.0))

def print_derived_intelligence(details: UnifiedDetails) -> None:
    """Optional, read-only display of the derived_metrics bundle.

    Renders only values already computed by derived_metrics_adapter; nothing
    here recalculates a score. Every lookup is a safe .get() so a missing or
    partial bundle simply prints fewer lines (or nothing at all).
    """
    dm = details.derived_metrics
    if not dm:
        return
    lines: list[str] = []

    headline = dm.get("headline") or {}
    ccs = headline.get("crowd_consensus_score")
    if ccs is not None:
        label = headline.get("label")
        tag = f" {D}({label}){Z}" if label else ""
        lines.append(f"  {Y}Crowd Consensus:{Z} {ccs}/100{tag}")
    if headline.get("summary"):
        lines.append(f"  {Y}Summary:{Z} {headline.get('summary')}")

    perf = dm.get("performance_score") or {}
    if perf:
        bits: list[str] = []
        if perf.get("score") is not None:
            bits.append(f"{perf.get('score')}/100")
        if perf.get("longevity_label"):
            bits.append(f"longevity {perf.get('longevity_label')}")
        if perf.get("sillage_label"):
            bits.append(f"sillage {perf.get('sillage_label')}")
        if bits:
            lines.append(f"  {Y}Performance:{Z} {' · '.join(bits)}")

    val = dm.get("value_score") or {}
    if val:
        bits = []
        if val.get("dominant_label"):
            bits.append(str(val.get("dominant_label")))
        if val.get("score") is not None:
            bits.append(f"{val.get('score')}/100")
        if bits:
            lines.append(f"  {Y}Value:{Z} {' '.join(bits)}")

    wear = dm.get("wear_profile") or {}
    if wear:
        bits = []
        seasons = wear.get("primary_seasons") or []
        if seasons:
            bits.append(", ".join(seasons))
        if wear.get("primary_time"):
            bits.append(str(wear.get("primary_time")))
        if bits:
            lines.append(f"  {Y}Wear:{Z} {' · '.join(bits)}")

    community = dm.get("community_interest_score") or {}
    if community.get("score") is not None:
        lines.append(f"  {Y}Community:{Z} {community.get('score')}/100")

    accords = dm.get("main_accords") or {}
    if accords.get("accord_summary"):
        lines.append(f"  {Y}Main Profile:{Z} {accords.get('accord_summary')}")

    if not lines:
        return
    print(f" {B}✦ DERIVED INTELLIGENCE ✦{Z}")
    for line in lines:
        print(line)
    print()

def print_dashboard(frag: UnifiedFragrance, details: UnifiedDetails, fetch_time: float, debug: bool = False) -> None:
    width = 80
    print(f"\n{B}{C}╭{'─' * (width + 2)}╮{Z}")
    title = frag.name.upper()[:width]
    print(f"{C}│{Z} {B}{title}{Z}{' ' * (width - len(title))} {C}│{Z}")
    house, year = frag.brand[:30], (frag.year or "N/A")[:10]
    house_raw, year_raw = f"House: {house}", f"Year: {year}"
    spacing = max(1, width - len(house_raw) - len(year_raw))
    print(f"{C}│{Z} {Y}House:{Z} {house}{' ' * spacing}{Y}Year:{Z} {year} {C}│{Z}")
    gender = details.gender[:60]
    print(f"{C}│{Z} {Y}Gender:{Z} {gender}{' ' * max(0, width - len('Gender: ') - len(gender))} {C}│{Z}")
    print(f"{B}{C}╰{'─' * (width + 2)}╯{Z}")
    if debug:
        print(f" {D}↳ ref 1: {frag.bn_url or 'N/A'}{Z}")
        print(f" {D}↳ ref 2: {frag.frag_url or 'N/A'}{Z}")
        print(f" {D}↳ Resolver: {frag.resolver_source or 'N/A'} {frag.resolver_score:.2f}{Z}")
    print(f" {D}[Selected-detail data synchronized in {fetch_time:.2f}s]{Z}\n")
    if details.bn_consensus:
        print(f" {B}✦ COMMUNITY CONSENSUS ✦{Z}")
        for key, label, color in (("pos", "Positive", G), ("neu", "Neutral ", Y), ("neg", "Negative", R)):
            votes, pct = details.bn_consensus.get(key, (0, 0))
            print(f"  {label}: {draw_bar(pct, 15, color)} {pct:>3}% {D}({votes} votes){Z}")
        print()
    print_scorecard(details)
    print_derived_intelligence(details)
    print_fragrantica_metric_groups(details)
    if details.pros_cons:
        print(f" {B}✦ WHAT PEOPLE SAY / PROS & CONS ✦{Z}")
        for item in details.pros_cons[:6]:
            print(f"  • {item}")
        print()
    print(f" {B}✦ OLFACTORY NOTES ✦{Z}")
    if details.notes.has_pyramid:
        if details.notes.top: print(f"  {G}Top:{Z}    {', '.join(details.notes.top)}")
        if details.notes.heart: print(f"  {Y}Heart:{Z}  {', '.join(details.notes.heart)}")
        if details.notes.base: print(f"  {R}Base:{Z}   {', '.join(details.notes.base)}")
    elif details.notes.flat:
        print(f"  {C}Notes:{Z}  {', '.join(details.notes.flat)}")
    else:
        print(f"  {D}Notes structure unavailable from both sources.{Z}")
    print()

def fit_cell(text: object, width: int) -> str:
    raw = TextSanitizer.clean(text)
    if len(raw) > width:
        return raw[: max(0, width - 1)] + "…"
    return raw.ljust(width)

def print_results_table(results: list[UnifiedFragrance], search_time: float, debug: bool = False) -> None:
    linked_count = sum(1 for item in results if item.frag_url)
    print(f"\n{G}✓ {len(results)} results · {linked_count} ★ linked · {search_time:.2f}s{Z}")
    print(f"{D}Sorted by query match. Sent % shows positive review sentiment; it is not the ranking key.{Z}\n")
    headers = [("#", 2), ("Match", 5), ("Sent %", 10), ("★", 3), ("Fragrance", 32), ("Brand", 22), ("Year", 6)]
    if debug:
        headers.append(("Resolver", 12))
    line = "┬".join("─" * (w + 2) for _, w in headers)
    mid = "┼".join("─" * (w + 2) for _, w in headers)
    bot = "┴".join("─" * (w + 2) for _, w in headers)
    print(f"{B}{C}╭{line}╮{Z}")
    print(f"{B}{C}│{Z}" + f"{B}{C}│{Z}".join(f" {fit_cell(h, w)} " for h, w in headers) + f"{B}{C}│{Z}")
    print(f"{B}{C}├{mid}┤{Z}")
    for index, item in enumerate(results, 1):
        row = [
            (f"{index:02d}", 2),
            (match_cell(item), 5),
            (sentiment_cell(item), 10),
            (link_cell(item), 3),
            (item.name, 32),
            (item.brand, 22),
            (item.year or "—", 6),
        ]
        if debug:
            row.append((item.resolver_source or "—", 12))
        print(f"{C}│{Z}" + f"{C}│{Z}".join(f" {fit_cell(v, w)} " for v, w in row) + f"{C}│{Z}")
    print(f"{B}{C}╰{bot}╯{Z}")

def _search_core(scraper, query: str, args, *, allow_repair: bool) -> tuple[list[UnifiedFragrance], list[UnifiedFragrance], list[UnifiedFragrance]]:
    timer = StageTimer(enabled=args.debug_timing)
    trace = ResolverTrace(enabled=args.debug_timing)
    cache = IdentityCache(args.fg_cache)
    shared_deadline = Deadline(args.initial_timeout)
    def fg_search_job() -> list[UnifiedFragrance]:
        token = trace.start("raw_search", query=query, deadline=shared_deadline)
        rows = FragranticaEngine.extract_search_data(
            scraper,
            query,
            max_results=args.max_frag_results,
            deadline=shared_deadline,
        )
        trace.finish(token, result_count=len(rows), linked_count=sum(1 for item in rows if item.frag_url), skipped_reason="" if rows else "no_perfume_links_found", deadline=shared_deadline)
        return rows
    initial = bounded_parallel(
        {
            "bn": lambda: BasenotesEngine.extract_search_data(scraper, query, deadline=shared_deadline),
            "fg": fg_search_job,
        },
        seconds=args.initial_timeout,
        max_workers=2,
    )
    bn_results = initial.get("bn") or []
    frag_results = initial.get("fg") or []
    timer.mark("initial_fg_bn_fetch")
    print(f"{D}[SYS] First-pass resolver: {len(bn_results)} + {len(frag_results)} candidate links.{Z}")
    candidates = Orchestrator.match_and_merge(bn_results, frag_results)[: args.max_results]
    for item in candidates:
        item.query_score = IdentityTools.relevance_score(query, item)
    candidates.sort(key=lambda item: (item.query_score, bool(item.frag_url), item.resolver_score), reverse=True)
    timer.mark("build_candidates")
    if allow_repair and QueryRepair.needs_repair(bn_results, candidates):
        trace.add_skip("raw_search", "raw_search_rejected_default_section", query=query, deadline=shared_deadline)
        timer.mark("repair_check")
        trace.add_skip("external_search", "disabled")
        trace.add_skip("browser_catalog", "disabled")
        trace.print_summary(candidates)
        return candidates, bn_results, frag_results
    if not candidates:
        trace.add_skip("designer_catalog", "no_candidates")
        trace.add_skip("external_search", "disabled")
        trace.add_skip("browser_catalog", "disabled")
        trace.print_summary([])
        return [], bn_results, frag_results
    cache_token = trace.start("fg_cache", query=f"{len(candidates)} candidates")
    cache_hits = SearchSniper.apply_cache(candidates, cache)
    trace.finish(cache_token, result_count=len(candidates), linked_count=cache_hits, skipped_reason="" if cache_hits else "no_cache_hits")
    if args.debug_timing and cache_hits:
        print(f"{D}[SYS] FG cache linked {cache_hits} candidate(s).{Z}")
    timer.mark("fg_cache_lookup")
    catalog_hits = SearchSniper.attach_from_designer_catalog(
        scraper,
        candidates,
        cache,
        budget_seconds=args.catalog_budget,
        max_workers=args.catalog_workers,
        slug_limit=args.catalog_slug_limit,
        trace=trace,
    )
    if args.debug_timing and catalog_hits:
        print(f"{D}[SYS] Designer catalog linked {catalog_hits} candidate(s).{Z}")
    timer.mark("designer_catalog_resolve")
    if args.related_budget > 0:
        related_hits = SearchSniper.attach_related_from_known_pages(
            scraper,
            candidates,
            cache,
            budget_seconds=args.related_budget,
            page_timeout=args.related_page_timeout,
            max_pages=args.related_max_pages,
            trace=trace,
        )
        if args.debug_timing and related_hits:
            print(f"{D}[SYS] Related known pages linked {related_hits} candidate(s).{Z}")
        timer.mark("related_known_pages")
    else:
        trace.add_skip("related_page_crawl", "budget_exhausted" if args.related_budget <= 0 else "disabled")
        timer.mark("related_known_pages", skipped=True)
    if args.external_search:
        sniper_hits = SearchSniper.attach_external_search(
            scraper,
            candidates,
            cache,
            budget_seconds=args.external_budget,
            max_workers=args.external_workers,
            trace=trace,
        )
        if args.debug_timing and sniper_hits:
            print(f"{D}[SYS] External sniper linked {sniper_hits} candidate(s).{Z}")
        timer.mark("external_sniper")
    else:
        trace.add_skip("external_search", "disabled")
        timer.mark("external_sniper", skipped=True)
    if args.metrics_budget > 0 and args.metrics_max > 0:
        metric_deadline = Deadline(args.metrics_budget)
        metric_targets = candidates[: max(1, min(args.metrics_max, len(candidates)))]
        def fetch_metric(item: UnifiedFragrance) -> None:
            BasenotesEngine.fetch_metrics(scraper, item, deadline=metric_deadline)
        run_budgeted_items(metric_targets, fetch_metric, seconds=args.metrics_budget, max_workers=args.metrics_workers)
        timer.mark("bn_metrics")
    else:
        timer.mark("bn_metrics", skipped=True)
    for item in candidates:
        item.query_score = IdentityTools.relevance_score(query, item)
    candidates.sort(key=lambda item: (item.query_score, bool(item.frag_url), item.resolver_score, item.bn_vote_count), reverse=True)
    SearchSniper.cache_successes(candidates, cache)
    trace.add_skip("browser_catalog", "disabled")
    timer.total("total_search")
    trace.print_summary(candidates)
    return candidates, bn_results, frag_results

def search_once(scraper, query: str, args) -> list[UnifiedFragrance]:
    candidates, bn_results, _ = _search_core(scraper, query, args, allow_repair=True)
    if QueryRepair.needs_repair(bn_results, candidates):
        if args.spell_repair_budget > 0:
            suggestion = QueryRepair.suggest(scraper, query, seconds=args.spell_repair_budget)
            if suggestion:
                print(f"{Y}[SYS] Search corrected: {query!r} → {suggestion!r}{Z}")
                repaired, repaired_bn, _ = _search_core(scraper, suggestion, args, allow_repair=False)
                if repaired and not QueryRepair.needs_repair(repaired_bn, repaired):
                    return repaired
        canonical_brand = IdentityTools.canonical_brand_query(query)
        if canonical_brand:
            catalog = SearchSniper.catalog_candidates_for_brand(
                scraper,
                canonical_brand,
                Deadline(args.catalog_budget),
                slug_limit=args.catalog_slug_limit,
            )
            brand_results: list[UnifiedFragrance] = []
            seen: set[str] = set()
            for item in catalog:
                if item.url in seen:
                    continue
                seen.add(item.url)
                frag = UnifiedFragrance(
                    name=item.name,
                    brand=item.brand,
                    year=item.year,
                    frag_url=item.url,
                    resolver_source="designer_catalog_brand_query",
                    resolver_score=0.98,
                    query_score=1.0,
                )
                brand_results.append(frag)
                if len(brand_results) >= args.max_results:
                    break
            if brand_results:
                return brand_results
        best = max((item.query_score for item in candidates), default=0.0)
        if best < QueryRepair.MIN_USEFUL_TOP_SCORE:
            print(f"{Y}[SYS] Ignored weak FG-only fallback rows; best match was only {int(best * 100)}%.{Z}")
            return []
    return candidates

def fetch_selected_details(scraper, selected: UnifiedFragrance, detail_timeout: float) -> UnifiedDetails:
    details = UnifiedDetails(notes=NotesList())
    deadline = Deadline(detail_timeout)
    bounded_parallel({
        "bn": lambda: BasenotesEngine.fetch_details(scraper, selected.bn_url, details, deadline=deadline),
        "fg": lambda: FragranticaEngine.fetch_details(scraper, selected.frag_url, details, deadline=deadline),
    }, seconds=detail_timeout, max_workers=2)
    normalize_notes(details.notes)
    details.reviews = dedupe_reviews(details.reviews)
    # FG + BN detail synchronization is complete; attach the normalized,
    # frontend-ready derived-metrics bundle without touching any raw fields.
    # Derived metrics are best-effort: a failure here must not abort the
    # raw detail fetch, so fall back to None on any import/build error.
    try:
        from derived_metrics_adapter import build_derived_metrics
        details.derived_metrics = build_derived_metrics(details)
    except Exception:
        details.derived_metrics = None
    return details

def parse_local_fragrantica_html(path: str, max_results: int = 25) -> list[UnifiedFragrance]:
    with open(path, "r", encoding="utf-8") as handle:
        markup = handle.read()
    return FragranticaEngine.parse_search_html(markup, max_results=max_results)

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fast dual-source fragrance parser.")
    parser.add_argument("query", nargs="?", help="Fragrance search query, e.g. 'Dior Sauvage'.")
    parser.add_argument(
        "--parse-fragrantica-html",
        dest="html_path",
        help="Parse a saved Fragrantica search HTML file instead of making network requests.",
    )
    parser.add_argument("--max-frag-results", type=int, default=25, help="Max native Fragrantica search cards to parse.")
    parser.add_argument("--max-results", type=int, default=15, help="Max merged candidates to display.")
    parser.add_argument("--initial-timeout", type=float, default=6.0, help="Shared deadline for first-pass search.")
    parser.add_argument("--detail-timeout", type=float, default=6.0, help="Shared deadline for selected detail fetch.")
    parser.add_argument("--metrics-budget", type=float, default=0.9, help="Shared budget for rating metrics. Use 0 to skip.")
    parser.add_argument("--metrics-workers", type=int, default=5)
    parser.add_argument("--metrics-max", type=int, default=5, help="Only fetch sentiment for the top N query-relevant rows before display.")
    parser.add_argument("--catalog-budget", type=float, default=3.5, help="Shared designer catalog resolver budget.")
    parser.add_argument("--catalog-workers", type=int, default=3)
    parser.add_argument("--catalog-slug-limit", type=int, default=6)
    parser.add_argument("--related-budget", type=float, default=0.8, help="Budget for related-link expansion. Use 0 to skip.")
    parser.add_argument("--related-page-timeout", type=float, default=2.2)
    parser.add_argument("--related-max-pages", type=int, default=3)
    parser.add_argument("--external-search", action="store_true", help="Enable opt-in open-web sniper fallback.")
    parser.add_argument("--external-budget", type=float, default=5.0)
    parser.add_argument("--external-workers", type=int, default=3)
    parser.add_argument("--spell-repair-budget", type=float, default=1.6, help="Google/Bing spelling repair budget, only used after a clearly bad first pass. Use 0 to disable.")
    parser.add_argument("--fg-cache", default=str(Path.home() / ".cache" / "fragrance_parser_fg_identity_cache_v2.json"))
    parser.add_argument("--debug", action="store_true", help="Print resolver diagnostics/timing trace. Alias for --debug-timing.")
    parser.add_argument("--debug-timing", action="store_true", help="Print timing checkpoints for each resolver stage.")
    parser.add_argument("--show-source-urls", action="store_true", help="Show source URLs and resolver source in selected dashboard.")
    return parser

def main() -> None:
    if getattr(sys.stdout, "encoding", None) and sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8" and hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = build_parser()
    args = parser.parse_args()
    if getattr(args, "debug", False):
        args.debug_timing = True
    if args.html_path:
        parsed = parse_local_fragrantica_html(args.html_path, max_results=args.max_frag_results)
        print(f"Parsed {len(parsed)} Fragrantica cards")
        for index, item in enumerate(parsed, 1):
            print(f"{index:02d}. {item.brand} | {item.name} | {item.year or 'N/A'} | {item.frag_url}")
        return
    current_query = args.query
    # Interactive launch: let the user pick search vs. the management dashboard.
    # A query passed on the CLI keeps today's straight-to-search behavior.
    if not current_query:
        mode = input(
            f"\n{B}🔎 [S]earch fragrances  ·  🛠  [M] SRT Set Engine  —  choose [S/m]: {Z}"
        ).strip().lower()
        if mode in {"m", "manage", "management", "srt"}:
            import importlib.util
            worker_path = Path(__file__).resolve().parent / "scripts" / "enrichment_worker.py"
            spec = importlib.util.spec_from_file_location("enrichment_worker", worker_path)
            worker = importlib.util.module_from_spec(spec)
            sys.modules["enrichment_worker"] = worker  # let @dataclass resolve its module
            spec.loader.exec_module(worker)
            worker.launch_dashboard()
            return
    scraper = get_scraper()
    while True:
        try:
            if not current_query:
                current_query = input(f"\n{B}{Y}🔎 Search Fragrance (or 'q' to quit): {Z}").strip()
            if current_query.lower() in {"quit", "exit", "q"}:
                sys.exit(0)
            start_time = time.time()
            print(f"\n{D}[SYS] Executing bounded dual-source resolver...{Z}")
            merged_results = search_once(scraper, current_query, args)
            search_time = time.time() - start_time
            if not merged_results:
                print(f"\n{R}❌ No fragrance candidates found for '{current_query}'.{Z}")
                current_query = None
                continue
            while True:
                print_results_table(merged_results, search_time, debug=args.debug_timing or args.show_source_urls)
                choice = input(f"\n{B}{C}➤ Select [1-{len(merged_results)}], [S]earch again, [Q]uit: {Z}").strip().lower()
                if choice in {"q", "quit", "exit"}:
                    sys.exit(0)
                if choice in {"s", "search"}:
                    current_query = None
                    break
                if not choice:
                    continue
                try:
                    index = int(choice)
                except ValueError:
                    continue
                if not 1 <= index <= len(merged_results):
                    continue
                selected = merged_results[index - 1]
                fetch_start = time.time()
                details = fetch_selected_details(scraper, selected, detail_timeout=args.detail_timeout)
                print_dashboard(selected, details, time.time() - fetch_start, debug=args.debug_timing or args.show_source_urls)
                if details.description:
                    view_desc = input(f"{B}{C}➤ View full description? [y/N]: {Z}").strip().lower()
                    if view_desc in {"y", "yes"}:
                        print(f"\n{B}✦ DESCRIPTION ✦{Z}\n{D}{'─' * 74}{Z}\n{details.description}\n{D}{'─' * 74}{Z}\n")
                if details.reviews:
                    view_reviews = input(f"{B}{C}➤ Show sample reviews too? [y/N]: {Z}").strip().lower()
                    if view_reviews in {"y", "yes"}:
                        print_reviews_by_source(details.reviews)
                input(f"\n{D}Press Enter to return to the search list...{Z}")
        except KeyboardInterrupt:
            print(f"\n{Y}Session terminated by user.{Z}")
            sys.exit(0)

if __name__ == "__main__":
    main()
