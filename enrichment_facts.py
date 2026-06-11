"""Pure helpers for fragrance enrichment facts.

These helpers keep default display strings out of completeness decisions and
derive app-facing taxonomy/context from the engine's existing derived metrics.
"""

from __future__ import annotations

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


def primary_season_from_wear_profile(wear_profile: Any) -> str | None:
    if not isinstance(wear_profile, dict):
        return None
    seasons = wear_profile.get("primary_seasons")
    if isinstance(seasons, list):
        for season in seasons:
            if is_fact_complete(season, "season"):
                return str(season).strip()
    return None
