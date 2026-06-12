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


def top_accords_from(source: Any) -> list[str]:
    dm = _derived_metrics_from(source)
    main = dm.get("main_accords") if isinstance(dm, dict) else None
    if not isinstance(main, dict):
        return []
    top = main.get("top_accords")
    if isinstance(top, list):
        return [str(item).strip() for item in top if str(item or "").strip()]
    vector = main.get("scent_vector")
    if isinstance(vector, list):
        ordered = sorted(
            (item for item in vector if isinstance(item, dict)),
            key=lambda item: float(item.get("score") or 0),
            reverse=True,
        )
        return [
            str(item.get("accord") or "").strip()
            for item in ordered
            if str(item.get("accord") or "").strip()
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
