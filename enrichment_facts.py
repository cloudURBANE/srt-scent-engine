"""Pure helpers for fragrance enrichment facts.

These helpers keep default display strings out of completeness decisions and
derive app-facing taxonomy/context from the engine's existing derived metrics.
"""

from __future__ import annotations

import re
from typing import Any

UNKNOWN_TEXT = {"", "unknown", "n/a", "none", "null"}

FIELD_DEFAULTS = {
    "gender": {"unisex / unspecified"},
    "concentration": {"unknown"},
    "family": {"unknown family", "unknown"},
    "primary_family": {"unknown family", "unknown"},
    "season": {"universal"},
    "environment": {"universal"},
}

ACCORD_FAMILY_MAP = {
    "aldehydic": "Floral",
    "amber": "Amber",
    "ambery": "Amber",
    "animalic": "Musk",
    "aquatic": "Aquatic",
    "aromatic": "Aromatic",
    "balsamic": "Amber",
    "cacao": "Gourmand",
    "caramel": "Gourmand",
    "cherry": "Fruity",
    "chocolate": "Gourmand",
    "cinnamon": "Spicy",
    "citrus": "Citrus",
    "coffee": "Gourmand",
    "earthy": "Woody",
    "floral": "Floral",
    "fresh": "Fresh",
    "fresh spicy": "Spicy",
    "fruity": "Fruity",
    "green": "Green",
    "honey": "Gourmand",
    "iris": "Floral",
    "lactonic": "Gourmand",
    "lavender": "Aromatic",
    "leather": "Leather",
    "marine": "Aquatic",
    "mossy": "Chypre",
    "musky": "Musk",
    "oud": "Woody",
    "patchouli": "Chypre",
    "powdery": "Musk",
    "rose": "Floral",
    "rum": "Gourmand",
    "smoky": "Woody",
    "soft spicy": "Spicy",
    "sweet": "Gourmand",
    "tobacco": "Amber",
    "tuberose": "Floral",
    "vanilla": "Gourmand",
    "warm spicy": "Spicy",
    "white floral": "Floral",
    "woody": "Woody",
    "yellow floral": "Floral",
}


def expand_raw_accords(accords: Any) -> list[str]:
    """Flatten raw source accords into the engine's flat accord vocabulary.

    Parfumo reports compound accords like "fruity-sweet" / "synthetic-aquatic";
    ACCORD_FAMILY_MAP and the rest of the pipeline speak Fragrantica's flat
    tokens, so compounds are split on hyphens/slashes. Order-preserving,
    deduplicated, lowercase.
    """
    if not isinstance(accords, (list, tuple)):
        return []
    out: list[str] = []
    for accord in accords:
        for token in re.split(r"[-/]", str(accord or "")):
            token = token.strip().lower()
            if token and token not in out:
                out.append(token)
    return out


def is_fact_complete(value: Any, field: str | None = None) -> bool:
    """Return True only for real facts, not UI/default sentinels."""
    if value is None:
        return False
    if isinstance(value, (list, tuple, set)):
        return any(is_fact_complete(item, field) for item in value)
    if isinstance(value, dict):
        return any(is_fact_complete(item, field) for item in value.values())

    text = str(value).strip()
    if not text:
        return False
    lowered = text.lower()
    if lowered in UNKNOWN_TEXT:
        return False
    if field and lowered in FIELD_DEFAULTS.get(field, set()):
        return False
    return True


def _derived_metrics_from(source: Any) -> dict[str, Any]:
    if isinstance(source, dict):
        if isinstance(source.get("derived_metrics"), dict):
            return source["derived_metrics"]
        return source
    dm = getattr(source, "derived_metrics", None)
    return dm if isinstance(dm, dict) else {}


# Vote-count tokens ("14.9K", "11.2K", "6.5K", "1,204") and love/hate rating
# words ("Hate", "Like", ...) leak into Fragrantica's "Main accords" card when
# the scraper's container detection over-captures the adjacent Rating
# Distribution / Community Interest widgets. Because those bars carry high
# percentages they sort ABOVE the real accords and crowd them off the card
# (the "Dylan Blue only shows two accords" bug). A real accord is always an
# alphabetic label -- never a number, never a love/hate rating bucket -- so
# these are unambiguously junk and safe to drop on every read/write path.
_VOTE_COUNT_RE = re.compile(r"^\d[\d.,]*\s*[kmb]?$", re.IGNORECASE)
_RATING_WORD_LABELS = {"love", "like", "ok", "okay", "dislike", "hate", "neutral"}
# "When to wear" widget buckets -- seasons and time-of-day -- that bleed in from
# the same over-capture. None of these is ever a real Fragrantica accord.
_SEASON_TIME_LABELS = {
    "spring", "summer", "fall", "autumn", "winter", "day", "night",
}
# Shopping/ad copy that the scraper occasionally swept up ("Many Items For Sale
# On ..."). No accord ever contains these phrases.
_AD_TEXT_MARKERS = ("for sale", "items for sale", "in stock", "shop ", " ad")
_AD_TEXT_LABELS = {"sponsored"}


def is_junk_accord_label(label: Any) -> bool:
    """True when an 'accord' label is actually scraped noise rather than a scent.

    Catches the three leaks that the Fragrantica "Main accords" card picks up when
    its container over-captures adjacent widgets: vote-count tokens (14.9K), the
    love/hate rating buckets (Hate, Like), the "When to wear" season/time buckets
    (Summer, Night), and stray shopping copy. A real accord is always an
    alphabetic scent label, so all of these are unambiguously junk.
    """
    s = str(label or "").strip()
    if not s:
        return True
    if _VOTE_COUNT_RE.match(s):
        return True
    low = s.lower()
    if low in _RATING_WORD_LABELS or low in _SEASON_TIME_LABELS or low in _AD_TEXT_LABELS:
        return True
    if any(marker in low for marker in _AD_TEXT_MARKERS):
        return True
    return False


def sanitize_derived_metrics(derived_metrics: Any) -> bool:
    """Strip scraped-junk accord labels from a stored derived_metrics blob, in place.

    ``build_derived_metrics`` drops junk (vote counts like 14.9K, the Hate/Like
    rating buckets, season/time buckets, and "sponsored" ad copy) at *compute*
    time, but any blob that was stored before a given junk label entered
    :func:`is_junk_accord_label` keeps it baked into ``main_accords``.
    :func:`top_accords_from` re-filters the plain ``top_accords`` *list* on every
    read -- yet nothing ever re-filtered the scored ``scent_vector``, and the SPA
    accord card renders its percentages from ``scent_vector``. That is how
    "sponsored 100%" kept showing on the card long after the filter learned the
    label: the clean list was served alongside a dirty vector.

    This is the read/projection-time backstop that converges any stored blob onto
    the clean accord set without a network recompute. It mutates ``main_accords``
    in place (the blob is always a freshly deserialized copy at the call sites)
    and returns ``True`` when it removed at least one junk entry.
    """
    if not isinstance(derived_metrics, dict):
        return False
    main = derived_metrics.get("main_accords")
    if not isinstance(main, dict):
        return False
    changed = False
    vector = main.get("scent_vector")
    if isinstance(vector, list):
        cleaned = [
            item
            for item in vector
            if isinstance(item, dict) and not is_junk_accord_label(item.get("accord"))
        ]
        if len(cleaned) != len(vector):
            # Re-sort so the highest-scoring real accord leads the card now that
            # a junk entry (which sorted to the top on its inflated %) is gone.
            cleaned.sort(key=lambda item: float(item.get("score") or 0), reverse=True)
            main["scent_vector"] = cleaned
            changed = True
    top = main.get("top_accords")
    if isinstance(top, list):
        cleaned_top = [
            s
            for item in top
            if (s := str(item).strip()) and not is_junk_accord_label(s)
        ]
        if cleaned_top != [str(item).strip() for item in top]:
            main["top_accords"] = cleaned_top
            changed = True
    return changed


def top_accords_from(source: Any) -> list[str]:
    dm = _derived_metrics_from(source)
    main = dm.get("main_accords") if isinstance(dm, dict) else None
    if not isinstance(main, dict):
        return []
    top = main.get("top_accords")
    if isinstance(top, list):
        return [
            s
            for item in top
            if (s := str(item).strip()) and not is_junk_accord_label(s)
        ]
    vector = main.get("scent_vector")
    if isinstance(vector, list):
        ordered = sorted(
            (item for item in vector if isinstance(item, dict)),
            key=lambda item: float(item.get("score") or 0),
            reverse=True,
        )
        return [
            s
            for item in ordered
            if (s := str(item.get("accord") or "").strip())
            and not is_junk_accord_label(s)
        ]
    return []


def derive_families(source: Any, existing_family: Any = None) -> dict[str, Any]:
    """Map main accords to canonical family fields with provenance."""
    accords = top_accords_from(source)
    families: list[str] = []
    for accord in accords:
        family = ACCORD_FAMILY_MAP.get(accord.strip().lower())
        if family and family not in families:
            families.append(family)

    if not families and is_fact_complete(existing_family, "family"):
        families = [str(existing_family).strip()]
        source_name = "existing_family"
        confidence = 60
    elif families:
        source_name = "main_accords_mapping"
        confidence = 85
    else:
        return {
            "primary_family": None,
            "families": [],
            "accords": accords,
            "provenance": None,
        }

    return {
        "primary_family": families[0],
        "families": families,
        "accords": accords,
        "provenance": {"source": source_name, "confidence": confidence},
    }


def derive_wear_profile(source: Any) -> dict[str, Any] | None:
    dm = _derived_metrics_from(source)
    wear = dm.get("wear_profile") if isinstance(dm, dict) else None
    return wear if isinstance(wear, dict) and wear else None


# Canonical, ordered list of per-fragrance facts the self-healing sweep audits.
# Every name here is granular on purpose: a sweep response says exactly which
# facts are missing for each fragrance, down to reviews.
FACT_FIELDS = (
    "name",
    "house",
    "year",
    "gender",
    "concentration",
    "family",
    "main_accords",
    "wear_profile",
    "performance_score",
    "value_score",
    "community_interest_score",
    "notes",
    "reviews",
)

_COVERAGE_FACTS = ("performance_score", "value_score", "community_interest_score")

# Facts a terminal-partial source can never deliver, keyed by the stored row's
# fg_raw["source"]. The heal sweep skips requeueing a row when every missing
# fact is listed here for its source: re-running the worker cannot improve such
# a row, it only burns resolver budget until the requested_count churn guard
# trips. Facts NOT listed (e.g. concentration, year, gender, notes, family,
# main_accords) stay heal-worthy for that source.
# A ``year_meta`` blob carrying this source marker means an authoritative
# fragrance DB (Parfumo/Fragrantica) *explicitly* states the release year is
# unknown -- a durable, page-determined fact rather than a not-yet-fetched gap.
# The worker stops requeueing for such a year, and the API surfaces it so the SPA
# can render an explicit "Unknown" instead of a blank-looking metric. The marker
# string itself is minted in year_resolver.extract_year_unknown_signal.
YEAR_AUTHORITATIVE_UNKNOWN_SOURCE = "decodo_serp_authoritative_unknown"


def year_meta_marks_unknown(year_meta: Any) -> bool:
    """True when ``year_meta`` authoritatively states the release year is unknown."""
    return (
        isinstance(year_meta, dict)
        and year_meta.get("source") == YEAR_AUTHORITATIVE_UNKNOWN_SOURCE
    )


SOURCE_UNSUPPLIABLE_FACTS: dict[str, frozenset[str]] = {
    # Parfumo pages have no FG status pyramid (wear votes, performance/value/
    # community-interest distributions) and review text is never ingested.
    "parfumo": frozenset(
        {
            "wear_profile",
            "performance_score",
            "value_score",
            "community_interest_score",
            "reviews",
        }
    ),
}

# Facts whose presence is decided entirely by what a *successfully parsed* source
# page exposes. Once a row is metrics-complete (the page fetched + the four
# derived-metric groups parsed), these either came through or the page simply
# has no community vote / review data -- which a re-fetch of the same URL cannot
# change. Treating them as heal-worthy is what made the queue churn forever:
# every drain re-scraped the same review-less page up to the requeue cap. They
# are deliberately NARROW -- year, gender, concentration, family, main_accords,
# and notes are NOT here, because a partial first parse can genuinely improve
# those on retry. See memory: quality-status-vs-fact-completeness-gate.
PAGE_DETERMINED_FACTS: frozenset[str] = frozenset(
    {
        "reviews",
        "wear_profile",
        "performance_score",
        "value_score",
        "community_interest_score",
    }
)


def record_source(record: dict[str, Any]) -> str:
    """The stored row's enrichment source ("" when unrecorded / FG-native)."""
    fg_raw = record.get("fg_raw") if isinstance(record.get("fg_raw"), dict) else {}
    return str(fg_raw.get("source") or "").strip().lower()


def _notes_have_content(notes: Any) -> bool:
    if not isinstance(notes, dict):
        return False
    return any(
        isinstance(notes.get(tier), list) and any(str(n or "").strip() for n in notes[tier])
        for tier in ("top", "heart", "base", "flat")
    )


def _reviews_have_content(*review_lists: Any) -> bool:
    for reviews in review_lists:
        if not isinstance(reviews, list):
            continue
        for review in reviews:
            if isinstance(review, dict) and str(review.get("text") or "").strip():
                return True
    return False


def record_fact_status(record: dict[str, Any]) -> dict[str, bool]:
    """Granular completeness per fact for a fragrance_records-shaped dict.

    Accepts the dict produced by db._fragrance_record_to_dict (keys: name,
    house, year, gender, fg_raw, bn_raw, derived_metrics). Default sentinels
    (`Unknown`, `Unknown Family`, `Unisex / Unspecified`, unsupported
    `Universal`) count as missing, matching is_fact_complete.
    """
    fg_raw = record.get("fg_raw") if isinstance(record.get("fg_raw"), dict) else {}
    bn_raw = record.get("bn_raw") if isinstance(record.get("bn_raw"), dict) else {}
    raw_identity = fg_raw.get("raw_identity")
    if not isinstance(raw_identity, dict):
        raw_identity = {}
    dm = record.get("derived_metrics")
    coverage = dm.get("source_coverage") if isinstance(dm, dict) else None
    if not isinstance(coverage, dict):
        coverage = {}

    concentration = fg_raw.get("concentration") or raw_identity.get("concentration")
    families = derive_families(record, existing_family=None)
    wear = derive_wear_profile(record)
    wear_complete = bool(
        wear
        and (
            primary_season_from_wear_profile(wear)
            or is_fact_complete(wear.get("primary_time"), "season")
        )
    )

    status = {
        "name": is_fact_complete(record.get("name")),
        "house": is_fact_complete(record.get("house")),
        "year": is_fact_complete(record.get("year"), "year"),
        "gender": is_fact_complete(record.get("gender"), "gender")
        or is_fact_complete(raw_identity.get("gender"), "gender"),
        "concentration": is_fact_complete(concentration, "concentration"),
        "family": is_fact_complete(families.get("primary_family"), "family"),
        "main_accords": bool(top_accords_from(record)),
        "wear_profile": wear_complete,
        "notes": _notes_have_content(fg_raw.get("notes"))
        or _notes_have_content(bn_raw.get("notes")),
        "reviews": _reviews_have_content(fg_raw.get("reviews"), bn_raw.get("reviews")),
    }
    for group in _COVERAGE_FACTS:
        status[group] = bool(coverage.get(group))
    return {field: status[field] for field in FACT_FIELDS}


def missing_facts(record: dict[str, Any]) -> list[str]:
    """Ordered names of every incomplete fact for one fragrance record."""
    status = record_fact_status(record)
    return [field for field in FACT_FIELDS if not status[field]]


def primary_season_from_wear_profile(wear_profile: Any) -> str | None:
    if not isinstance(wear_profile, dict):
        return None
    seasons = wear_profile.get("primary_seasons")
    if isinstance(seasons, list):
        for season in seasons:
            if is_fact_complete(season, "season"):
                return str(season).strip()
    return None


# --- Non-perfume detection ---------------------------------------------------
# The app is a fragrance app; body-care products (lotions, mists, washes,
# candles) are out of scope. This is a HIGH-PRECISION detector used both as an
# ingest gate (skip enqueuing non-perfumes) and to generate cleanup candidates.
# It must never flag a genuine perfume -- false positives delete real data.

# Brands whose catalog reaching this pipeline is overwhelmingly scented
# body-care, not eau de parfum. A brand only belongs here when the real-perfume
# guard below still protects its rare genuine fragrance (which carries an
# FG/Parfumo page). Keep this list conservative and curated.
NON_PERFUME_BRANDS = frozenset(
    {
        "bath and body works",
        "bath body works",
        "pull bear",
        "pull and bear",
        "the body shop",
        "body shop",
    }
)

# Unambiguous non-perfume product tokens. DELIBERATELY excludes words real
# perfumes use as names (e.g. Maison Margiela Replica "Bubble Bath",
# "Beach Walk", "Coffee Break"), so a token match is always a true non-perfume.
NON_PERFUME_TOKENS = (
    "body lotion",
    "body cream",
    "body butter",
    "body wash",
    "shower gel",
    "hand soap",
    "hand cream",
    "hand wash",
    "body scrub",
    "sugar scrub",
    "shampoo",
    "conditioner",
    "deodorant",
    "room spray",
    "scented candle",
    "wax melt",
    "reed diffuser",
)

_TEST_IDENTITY_MARKERS = ("dummy", "empty-notes", "uncached")


def _has_real_perfume_page(*urls: Any) -> bool:
    """True when any URL is a genuine Fragrantica/Parfumo *perfume* page.

    Pure string check (no engine import, avoids a circular dependency): a real
    Fragrantica perfume URL contains ``/perfume/`` and a Parfumo one
    ``/Perfumes/``. Basenotes-only or empty URLs are NOT a real-perfume page --
    that is exactly the never-resolved state the brand/token guards then judge.
    """
    for url in urls:
        text = str(url or "").lower()
        if "fragrantica.com/perfume/" in text or ("parfumo." in text and "/perfumes/" in text):
            return True
    return False


def non_perfume_signal(
    name: Any,
    house: Any,
    *,
    fg_url: Any = "",
    parfumo_url: Any = "",
    bn_url: Any = "",
) -> tuple[bool, str]:
    """(is_candidate, reason) -- conservative non-perfume classifier.

    Never flags an item that resolved to a real perfume page. Otherwise flags
    when the name carries an unambiguous non-perfume product token, when the
    identity is obviously test/placeholder data, or when the brand is a known
    body-care catalog. Returns ("", reason) so callers can log why.
    """
    name_l = str(name or "").strip().lower()
    house_l = re.sub(r"[^a-z0-9 ]", " ", str(house or "").lower())
    house_l = re.sub(r"\s+", " ", house_l).strip()

    # Test/placeholder identities are always junk, regardless of URL.
    blob = f"{name_l} {str(fg_url or '')} {str(bn_url or '')}".lower()
    if any(marker in blob for marker in _TEST_IDENTITY_MARKERS):
        return True, "test/placeholder identity"

    # A genuine perfume page is decisive: never flag it.
    if _has_real_perfume_page(fg_url, parfumo_url):
        return False, ""

    for token in NON_PERFUME_TOKENS:
        if token in name_l:
            return True, f"non-perfume product token: {token!r}"

    if house_l in NON_PERFUME_BRANDS:
        return True, "body-care brand, no real perfume page"

    return False, ""
