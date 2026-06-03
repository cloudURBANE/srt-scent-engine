"""Parfinity catalog helpers.

This module is intentionally HTTP/cache only. Request paths should read the
bundled JSON cache via ``load_cache`` and must not fetch the bulk endpoint.
"""
from __future__ import annotations

import html
import json
import os
import re
import unicodedata
import urllib.request
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import fragrance_parser_full_rewrite_fixed as engine

PARFINITY_HOSTS = {"www.parfinity.com", "parfinity.com"}
DEFAULT_LOCALE = "en"
DEFAULT_CACHE_PATH = Path(__file__).resolve().parent / "fg_cache" / "parfinity_cache.json"
_BULK_URL = "https://www.parfinity.com/api/products/bulk?locale={locale}"
_UA = {"User-Agent": "Mozilla/5.0 (compatible; scentcast-parfinity-cache/1.0)"}

_CACHE_INDEXES_BY_PATH: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}

_MD_LINK_RE = re.compile(r"\[([^\]]+)\]\([^)]+\)")
_TAG_RE = re.compile(r"<[^>]+>")
_SPACE_RE = re.compile(r"\s+")


def is_parfinity_url(url: str) -> bool:
    parsed = urlparse((url or "").strip())
    return parsed.netloc.lower() in PARFINITY_HOSTS


def _handle_from_url_or_handle(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    if "://" not in text:
        text = text.strip("/")
        if "/" in text:
            text = text.rstrip("/").split("/")[-1]
        return text
    parsed = urlparse(text)
    parts = [part for part in parsed.path.split("/") if part]
    if "perfume" in parts:
        idx = parts.index("perfume")
        if idx + 1 < len(parts):
            return parts[idx + 1]
    return parts[-1] if parts else ""


def canonical_parfinity_url(url: str) -> str:
    handle = _handle_from_url_or_handle(url)
    if handle:
        return f"https://www.parfinity.com/{DEFAULT_LOCALE}/perfume/{handle}".rstrip("/")

    text = (url or "").strip()
    if not text:
        return ""
    if "://" in text:
        scheme, rest = text.split("://", 1)
        if "/" in rest:
            host, path = rest.split("/", 1)
            text = f"{scheme.lower()}://{host.lower()}/{path}"
        else:
            text = f"{scheme.lower()}://{rest.lower()}"
    return text.rstrip("/")


def fetch_bulk(locale: str = DEFAULT_LOCALE) -> list[dict[str, Any]]:
    req = urllib.request.Request(_BULK_URL.format(locale=locale), headers=_UA)
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if isinstance(payload, dict):
        products = payload.get("products") or []
    elif isinstance(payload, list):
        products = payload
    else:
        products = []
    return [p for p in products if isinstance(p, dict)]


def _cache_path(path: Path | None = None) -> Path:
    if path is not None:
        return Path(path)
    return Path(os.environ.get("PARFINITY_CACHE_PATH") or DEFAULT_CACHE_PATH)


def _identity_key(brand: str, name: str) -> str:
    norm_brand = _normalize_identity(brand)
    norm_name = _normalize_identity(name)
    return f"{norm_brand}|{norm_name}" if norm_brand and norm_name else ""


def _normalize_identity(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value or "")
    ascii_text = decomposed.encode("ascii", "ignore").decode("ascii")
    return _SPACE_RE.sub(" ", re.sub(r"[^a-zA-Z0-9]+", " ", ascii_text)).strip().lower()


def _note_name(raw: Any, locale: str = DEFAULT_LOCALE) -> str:
    if isinstance(raw, dict):
        name = raw.get(locale) or raw.get("en") or next(iter(raw.values()), "")
    else:
        name = raw
    return str(name or "").strip()


def _dedupe(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = _normalize_identity(value)
        if not value or key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _notes_from_product(product: dict[str, Any]) -> dict[str, Any]:
    top: list[str] = []
    heart: list[str] = []
    base: list[str] = []
    flat: list[str] = []
    for note in product.get("notes") or []:
        if isinstance(note, dict):
            name = _note_name(note.get("name"))
            level = str(note.get("pyramidLevel") or "").strip().lower()
        else:
            name = str(note or "").strip()
            level = ""
        if not name:
            continue
        if level in {"top", "head"}:
            top.append(name)
        elif level in {"middle", "heart"}:
            heart.append(name)
        elif level in {"base", "drydown"}:
            base.append(name)
        else:
            flat.append(name)

    has_pyramid = bool(top or heart or base)
    if has_pyramid:
        notes = {
            "has_pyramid": True,
            "top": _dedupe(top),
            "heart": _dedupe(heart),
            "base": _dedupe(base),
            "flat": _dedupe(flat),
        }
    else:
        notes = {
            "has_pyramid": False,
            "top": [],
            "heart": [],
            "base": [],
            "flat": _dedupe(flat),
        }
    return notes


def _clean_description(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = html.unescape(text)
    text = _TAG_RE.sub(" ", text)
    text = _MD_LINK_RE.sub(r"\1", text)
    return _SPACE_RE.sub(" ", text).strip()


def _image_url(product: dict[str, Any]) -> str:
    featured = product.get("featuredImage")
    if isinstance(featured, dict):
        url = str(featured.get("url") or "").strip()
        if url:
            return url
    for image in product.get("images") or []:
        if isinstance(image, dict):
            url = str(image.get("url") or image.get("src") or "").strip()
            if url:
                return url
    return ""


def product_type_to_concentration(product_type: str) -> str:
    text = _SPACE_RE.sub(" ", str(product_type or "").strip().lower())
    if not text:
        return "Unknown"
    compact = re.sub(r"[^a-z0-9]+", " ", text).strip()
    if "extrait" in compact or "extract" in compact:
        return "Extrait"
    if "eau de parfum" in compact or re.search(r"\bedp\b", compact):
        return "Eau de Parfum"
    if "eau de toilette" in compact or re.search(r"\bedt\b", compact):
        return "Eau de Toilette"
    if "eau de cologne" in compact or re.search(r"\bedc\b", compact):
        return "Eau de Cologne"
    if "eau fraiche" in compact or "eau fraiche" in text:
        return "Eau de Cologne"
    if "body spray" in compact or "body mist" in compact or "hair mist" in compact:
        return "Body Spray"
    if "elixir" in compact or "le parfum" in text or compact == "parfum":
        return "Parfum"
    if compact in {"perfume", "fragrance"}:
        return "Parfum"
    return "Unknown"


def _product_concentration(product: dict[str, Any]) -> str:
    for key in ("concentration", "product_type", "productType"):
        value = product.get(key)
        hit = product_type_to_concentration(str(value or ""))
        if hit != "Unknown":
            return hit
    if bool(product.get("extrait")):
        return "Extrait"
    return "Unknown"


def product_to_unified_details(product: dict[str, Any]) -> engine.UnifiedDetails:
    notes_raw = _notes_from_product(product)
    details = engine.UnifiedDetails(
        notes=engine.NotesList(
            has_pyramid=bool(notes_raw.get("has_pyramid")),
            top=list(notes_raw.get("top") or []),
            heart=list(notes_raw.get("heart") or []),
            base=list(notes_raw.get("base") or []),
            flat=list(notes_raw.get("flat") or []),
        ),
        description=_clean_description(
            product.get("description")
            or product.get("descriptionHtml")
            or product.get("body_html")
            or product.get("shortDescription")
            or product.get("scentDescription")
        ),
        frag_cards={},
    )
    setattr(details, "concentration", _product_concentration(product))
    return details


def product_to_cache_entry(product: dict[str, Any]) -> dict[str, Any]:
    handle = str(product.get("handle") or "").strip()
    canonical = canonical_parfinity_url(handle)
    notes = _notes_from_product(product)
    perfumer_handles = product.get("perfumerHandles") or []
    if not isinstance(perfumer_handles, list):
        perfumer_handles = []
    tags = product.get("tags") or []
    if not isinstance(tags, list):
        tags = []
    return {
        "canonical_fg_url": canonical,
        "canonical_parfinity_url": canonical,
        "fg_url": canonical,
        "name": str(product.get("title") or product.get("name") or "").strip(),
        "house": str(product.get("vendor") or product.get("brandName") or "").strip(),
        "image_url": _image_url(product),
        "source": "parfinity",
        "quality_status": "partial",
        "notes": notes,
        "description": _clean_description(
            product.get("description")
            or product.get("descriptionHtml")
            or product.get("body_html")
            or product.get("shortDescription")
            or product.get("scentDescription")
        ),
        "concentration": _product_concentration(product),
        "raw_identity": {
            "handle": handle,
            "tags": [str(tag) for tag in tags],
            "perfumer": ", ".join(str(p) for p in perfumer_handles if p),
            "perfumer_handles": [str(p) for p in perfumer_handles if p],
            "brand_handle": str(product.get("brandHandle") or "").strip(),
        },
        "frag_cards": {},
    }


def load_cache(path: Path | None = None) -> dict[str, dict[str, dict[str, Any]]]:
    cache_path = _cache_path(path).resolve()
    cache_key = str(cache_path)
    cached = _CACHE_INDEXES_BY_PATH.get(cache_key)
    if cached is not None:
        return cached

    by_url_handle: dict[str, dict[str, Any]] = {}
    by_identity: dict[str, dict[str, Any]] = {}
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}

    raw_entries = payload.get("entries", {}) if isinstance(payload, dict) else {}
    if not raw_entries and isinstance(payload, dict) and isinstance(payload.get("products"), list):
        raw_entries = {
            canonical_parfinity_url(str(p.get("handle") or "")): product_to_cache_entry(p)
            for p in payload.get("products") or []
            if isinstance(p, dict)
        }
    if isinstance(raw_entries, dict):
        for key, entry in raw_entries.items():
            if not isinstance(entry, dict):
                continue
            canonical = canonical_parfinity_url(
                str(entry.get("canonical_parfinity_url") or entry.get("canonical_fg_url") or key)
            )
            if not canonical:
                continue
            raw_identity = entry.get("raw_identity") or {}
            if not isinstance(raw_identity, dict):
                raw_identity = {}
            handle = str(raw_identity.get("handle") or _handle_from_url_or_handle(canonical))
            by_url_handle[canonical] = entry
            if handle:
                by_url_handle[handle] = entry
            identity = _identity_key(str(entry.get("house") or ""), str(entry.get("name") or ""))
            if identity:
                by_identity.setdefault(identity, entry)

    indexes = {"by_url_handle": by_url_handle, "by_identity": by_identity}
    _CACHE_INDEXES_BY_PATH[cache_key] = indexes
    return indexes


def lookup_detail(url_or_handle: str) -> dict[str, Any] | None:
    indexes = load_cache()
    canonical = canonical_parfinity_url(url_or_handle)
    handle = _handle_from_url_or_handle(url_or_handle)
    return indexes["by_url_handle"].get(canonical) or indexes["by_url_handle"].get(handle)


def lookup_concentration(brand: str, name: str) -> str | None:
    indexes = load_cache()
    entry = indexes["by_identity"].get(_identity_key(brand, name))
    if not entry:
        return None
    concentration = str(entry.get("concentration") or "").strip()
    return concentration if concentration and concentration != "Unknown" else None


def overlay_onto_details(details: engine.UnifiedDetails, entry: dict[str, Any]) -> bool:
    if not isinstance(entry, dict) or not entry:
        return False
    applied = False
    notes = details.notes
    if not (notes.top or notes.heart or notes.base or notes.flat):
        cached_notes = entry.get("notes") or {}
        if isinstance(cached_notes, dict):
            notes.has_pyramid = bool(cached_notes.get("has_pyramid", False))
            notes.top = list(cached_notes.get("top", []) or [])
            notes.heart = list(cached_notes.get("heart", []) or [])
            notes.base = list(cached_notes.get("base", []) or [])
            notes.flat = list(cached_notes.get("flat", []) or [])
            applied = bool(notes.top or notes.heart or notes.base or notes.flat)

    description = str(entry.get("description") or "").strip()
    if description and not str(details.description or "").strip():
        details.description = description
        applied = True

    concentration = str(entry.get("concentration") or "").strip()
    current_concentration = str(getattr(details, "concentration", "") or "").strip()
    if concentration and concentration != "Unknown" and not current_concentration:
        setattr(details, "concentration", concentration)
        applied = True

    if not isinstance(details.frag_cards, dict) or details.frag_cards:
        return applied
    details.frag_cards = {}
    return applied


def iter_entries(path: Path | None = None) -> list[dict[str, Any]]:
    """Return one cache entry per Parfinity product (deduped by handle)."""
    indexes = load_cache(path)
    seen_handles: set[str] = set()
    entries: list[dict[str, Any]] = []
    for key, entry in indexes["by_url_handle"].items():
        if not str(key).startswith("http"):
            continue
        raw_identity = entry.get("raw_identity") or {}
        if not isinstance(raw_identity, dict):
            raw_identity = {}
        handle = str(raw_identity.get("handle") or _handle_from_url_or_handle(str(key)))
        if not handle or handle in seen_handles:
            continue
        seen_handles.add(handle)
        entries.append(entry)
    return entries


def entries_by_brand(path: Path | None = None) -> dict[str, list[dict[str, Any]]]:
    """Group Parfinity catalog entries by house/brand label."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for entry in iter_entries(path):
        house = str(entry.get("house") or "").strip()
        if not house:
            continue
        grouped.setdefault(house, []).append(entry)
    for products in grouped.values():
        products.sort(key=lambda row: str(row.get("name") or "").lower())
    return dict(sorted(grouped.items(), key=lambda item: item[0].lower()))
