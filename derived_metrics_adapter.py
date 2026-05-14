"""
derived_metrics_adapter.py

Adapter / normalization layer.

It consumes the already-parsed fragrance detail object (`UnifiedDetails`,
produced by fragrance_parser_full_rewrite_fixed.py) and emits a single
frontend-ready bundle of derived, normalized metrics.

Scope boundaries (intentional):
  * NO scraping, NO HTML parsing, NO selector changes, NO raw source extraction.
  * This layer only *reads* the parsed object and reshapes / normalizes it.
  * It is duck-typed: any object exposing ``.notes``, ``.bn_consensus`` and
    ``.frag_cards`` works, so importing the heavy parser module is unnecessary.

Entry point:
  build_derived_metrics(details) -> dict

Conventions:
  * Display scores are rounded integers (key ``score``); the underlying float is
    preserved alongside (key ``score_raw``) so downstream maths stays precise.
  * Missing metric groups return ``None`` -- never a fake ``0``.
  * Raw vote counts are preserved for every metric category.
"""

from __future__ import annotations

import math
import re
from typing import Any

__all__ = ["build_derived_metrics"]


# ---------------------------------------------------------------------------
# Adequacy maps (label -> 0..100 quality-adjusted weight)
# ---------------------------------------------------------------------------

LONGEVITY_ADEQUACY: dict[str, int] = {
    "Very Weak": 20,
    "Weak": 40,
    "Moderate": 70,
    "Long Lasting": 90,
    "Eternal": 100,
}

SILLAGE_ADEQUACY: dict[str, int] = {
    "Intimate": 55,
    "Moderate": 80,
    "Strong": 85,
    "Enormous": 70,
}

VALUE_ADEQUACY: dict[str, int] = {
    "Way Overpriced": 20,
    "Overpriced": 40,
    "Ok": 60,
    "Good Value": 80,
    "Great Value": 100,
}

SEASON_LABELS: dict[str, str] = {
    "winter": "Winter",
    "spring": "Spring",
    "summer": "Summer",
    "autumn": "Autumn/Fall",
    "fall": "Autumn/Fall",
}

TIME_LABELS: dict[str, str] = {
    "day": "Day",
    "night": "Night",
}

# crowd_consensus_score component weights
HEADLINE_WEIGHTS: dict[str, float] = {
    "fg_rating_score": 0.45,
    "bn_sentiment_score": 0.20,
    "performance_score": 0.15,
    "value_score": 0.10,
    "community_interest_score": 0.10,
}


# ---------------------------------------------------------------------------
# Small parsing / lookup helpers
# ---------------------------------------------------------------------------

def _to_int(value: Any) -> int:
    """Best-effort vote-count parse: 1234, '1,234', '1,234 votes', '1.2k'."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value or "").strip().lower().replace(",", "")
    m = re.search(r"([\d.]+)\s*([km]?)", text)
    if not m:
        return 0
    num = float(m.group(1))
    mult = 1000 if m.group(2) == "k" else 1_000_000 if m.group(2) == "m" else 1
    return int(num * mult)


def _round(value: float | None) -> int | None:
    return None if value is None else int(round(value))


def _get_card(details: Any, *needles: str) -> list[dict[str, Any]]:
    """Case-insensitive substring lookup of a frag_cards entry. First match wins."""
    cards = getattr(details, "frag_cards", None) or {}
    needles_l = [n.lower() for n in needles]
    for name, metrics in cards.items():
        low = str(name).lower()
        if any(n in low for n in needles_l):
            return list(metrics or [])
    return []


def _metric_votes(metric: dict[str, Any]) -> int:
    return _to_int(metric.get("count"))


def _dominant_label(metrics: list[dict[str, Any]]) -> str | None:
    """Label carrying the highest vote count."""
    valid = [m for m in metrics if str(m.get("label", "")).strip()]
    if not valid:
        return None
    return str(max(valid, key=_metric_votes).get("label")).strip()


def _weighted_adequacy(
    metrics: list[dict[str, Any]],
    label_map: dict[str, int],
) -> tuple[float | None, int, dict[str, int]]:
    """Vote-weighted adequacy score.

    Returns (score 0..100 or None, total_weighted_votes, raw_votes_by_label).
    raw_votes_by_label always reflects every parsed row, even unmapped ones.
    """
    raw: dict[str, int] = {}
    weighted_sum = 0.0
    total = 0
    for m in metrics:
        label = str(m.get("label", "")).strip()
        if not label:
            continue
        votes = _metric_votes(m)
        raw[label] = votes
        adequacy = label_map.get(label)
        if adequacy is None:
            adequacy = next(
                (v for k, v in label_map.items() if k.lower() == label.lower()),
                None,
            )
        if adequacy is not None and votes > 0:
            weighted_sum += adequacy * votes
            total += votes
    if total <= 0:
        return None, 0, raw
    return weighted_sum / total, total, raw


# ---------------------------------------------------------------------------
# Group 2: fg_rating_score
# ---------------------------------------------------------------------------

def _fg_rating_score(details: Any) -> dict[str, Any] | None:
    """Prefer the full rating distribution; fall back to the average-only summary."""
    dist = _get_card(details, "rating distribution")
    if dist:
        votes_by_star: dict[int, int] = {}
        weighted_sum = 0.0
        total = 0
        for m in dist:
            star = m.get("value")
            try:
                star = int(star)
            except (TypeError, ValueError):
                mm = re.search(r"[1-5]", str(m.get("label", "")))
                star = int(mm.group()) if mm else None
            if star is None:
                continue
            votes = _metric_votes(m)
            votes_by_star[star] = votes
            weighted_sum += star * votes
            total += votes
        if total > 0:
            weighted_average = weighted_sum / total
            score = weighted_average / 5.0 * 100.0
            positive = votes_by_star.get(4, 0) + votes_by_star.get(5, 0)
            return {
                "score": _round(score),
                "score_raw": score,
                "basis": "rating_distribution",
                "weighted_average": round(weighted_average, 3),
                "positive_share": round(positive / total, 4),
                "total_votes": total,
                "raw_votes": {str(k): votes_by_star[k] for k in sorted(votes_by_star)},
            }

    # Fallback: "Fragrantica Rating" average-only summary card ("4.2/5").
    for m in _get_card(details, "fragrantica rating"):
        mm = re.search(r"([0-5](?:\.\d+)?)\s*/\s*5", str(m.get("value", "")))
        if mm:
            rating = float(mm.group(1))
            score = rating / 5.0 * 100.0
            return {
                "score": _round(score),
                "score_raw": score,
                "basis": "rating_summary",
                "weighted_average": rating,
                "positive_share": None,  # not derivable without a distribution
                "total_votes": _to_int(m.get("count")),
                "raw_votes": None,
            }
    return None


# ---------------------------------------------------------------------------
# Group 3: bn_sentiment_score
# ---------------------------------------------------------------------------

def _bn_sentiment_score(details: Any) -> dict[str, Any] | None:
    consensus = getattr(details, "bn_consensus", None) or {}
    if not consensus:
        return None
    pos_v, pos_p = consensus.get("pos", (0, 0))
    neu_v, neu_p = consensus.get("neu", (0, 0))
    neg_v, neg_p = consensus.get("neg", (0, 0))
    total = _to_int(pos_v) + _to_int(neu_v) + _to_int(neg_v)
    if total <= 0:
        return None
    score = (_to_int(pos_v) + 0.5 * _to_int(neu_v)) / total * 100.0
    return {
        "score": _round(score),
        "score_raw": score,
        "total_votes": total,
        "positive_pct": pos_p,
        "neutral_pct": neu_p,
        "negative_pct": neg_p,
        "raw_votes": {
            "positive": _to_int(pos_v),
            "neutral": _to_int(neu_v),
            "negative": _to_int(neg_v),
        },
    }


# ---------------------------------------------------------------------------
# Group 4: performance_score
# ---------------------------------------------------------------------------

def _performance_score(details: Any) -> dict[str, Any] | None:
    lon_metrics = _get_card(details, "longevity")
    sil_metrics = _get_card(details, "sillage", "projection")

    lon_score, lon_total, lon_raw = (
        _weighted_adequacy(lon_metrics, LONGEVITY_ADEQUACY) if lon_metrics else (None, 0, {})
    )
    sil_score, sil_total, sil_raw = (
        _weighted_adequacy(sil_metrics, SILLAGE_ADEQUACY) if sil_metrics else (None, 0, {})
    )
    if lon_score is None and sil_score is None:
        return None

    # 0.60 longevity / 0.40 sillage -- renormalized when one side is missing.
    parts: list[tuple[float, float]] = []
    if lon_score is not None:
        parts.append((lon_score, 0.60))
    if sil_score is not None:
        parts.append((sil_score, 0.40))
    total_w = sum(w for _, w in parts)
    score = sum(s * w for s, w in parts) / total_w

    return {
        "score": _round(score),
        "score_raw": score,
        "longevity_label": _dominant_label(lon_metrics),
        "sillage_label": _dominant_label(sil_metrics),
        "longevity_adequacy": round(lon_score, 3) if lon_score is not None else None,
        "sillage_adequacy": round(sil_score, 3) if sil_score is not None else None,
        "longevity_total_votes": lon_total or None,
        "sillage_total_votes": sil_total or None,
        "weight_basis": round(total_w, 2),
        "raw_votes": {"longevity": lon_raw, "sillage": sil_raw},
    }


# ---------------------------------------------------------------------------
# Group 5: value_score
# ---------------------------------------------------------------------------

def _value_score(details: Any) -> dict[str, Any] | None:
    metrics = _get_card(details, "price value", "value")
    if not metrics:
        return None
    score, total, raw = _weighted_adequacy(metrics, VALUE_ADEQUACY)
    if score is None:
        return None
    return {
        "score": _round(score),
        "score_raw": score,
        "dominant_label": _dominant_label(metrics),
        "total_votes": total,
        "raw_votes": raw,
    }


# ---------------------------------------------------------------------------
# Group 6: wear_profile  (NOT part of crowd_consensus_score)
# ---------------------------------------------------------------------------

def _wear_profile(details: Any) -> dict[str, Any] | None:
    metrics = _get_card(details, "when to wear", "season")
    if not metrics:
        return None

    seasons: list[tuple[str, int]] = []
    times: list[tuple[str, int]] = []
    for m in metrics:
        low = str(m.get("label", "")).strip().lower()
        votes = _metric_votes(m)
        if low in SEASON_LABELS:
            seasons.append((SEASON_LABELS[low], votes))
        elif low in TIME_LABELS:
            times.append((TIME_LABELS[low], votes))
    if not seasons and not times:
        return None

    season_total = sum(v for _, v in seasons)
    time_total = sum(v for _, v in times)

    primary_seasons: list[str] | None = None
    if seasons and season_total > 0:
        ordered = sorted(seasons, key=lambda x: x[1], reverse=True)
        primary_seasons = [ordered[0][0]]
        # If the top two seasons are within 5 percentage points, return both.
        if len(ordered) > 1:
            top_share = ordered[0][1] / season_total
            second_share = ordered[1][1] / season_total
            if top_share - second_share <= 0.05:
                primary_seasons.append(ordered[1][0])

    primary_time: str | None = None
    if times and time_total > 0:
        primary_time = max(times, key=lambda x: x[1])[0]

    return {
        "primary_seasons": primary_seasons,
        "primary_time": primary_time,
        "season_total_votes": season_total or None,
        "time_total_votes": time_total or None,
        "raw_votes": {
            "seasons": {name: v for name, v in seasons},
            "time": {name: v for name, v in times},
        },
    }


# ---------------------------------------------------------------------------
# Group 7: community_interest_score
# ---------------------------------------------------------------------------

def _community_interest_score(details: Any) -> dict[str, Any] | None:
    metrics = _get_card(details, "community interest", "wardrobe")
    if not metrics:
        return None

    counts: dict[str, int] = {}
    for m in metrics:
        key = str(m.get("value") or m.get("label", "")).strip().lower()
        if key:
            counts[key] = _metric_votes(m)

    have = counts.get("have", 0)
    had = counts.get("had", 0)
    want = counts.get("want", 0)
    community_action_total = have + had + want
    if community_action_total <= 0:
        return None

    want_ratio = want / (have + want) if (have + want) > 0 else None
    retention_ratio = have / (have + had) if (have + had) > 0 else None
    popularity_index = min(100.0, math.log10(community_action_total + 1) / 5 * 100)

    # Unit-consistent blend: every component is on a 0..100 scale before
    # weighting. want_ratio / retention_ratio are 0..1 fractions, so they are
    # scaled to their percentage equivalents here. Weights renormalize over
    # whichever components are actually available.
    want_ratio_pct = want_ratio * 100 if want_ratio is not None else None
    retention_ratio_pct = retention_ratio * 100 if retention_ratio is not None else None
    parts = [
        (popularity_index, 0.60),
        (want_ratio_pct, 0.25),
        (retention_ratio_pct, 0.15),
    ]
    present = [(v, w) for v, w in parts if v is not None]
    total_w = sum(w for _, w in present)
    score = sum(v * w for v, w in present) / total_w

    return {
        "score": _round(score),
        "score_raw": score,
        "community_action_total": community_action_total,
        "want_ratio": round(want_ratio, 4) if want_ratio is not None else None,
        "retention_ratio": round(retention_ratio, 4) if retention_ratio is not None else None,
        "want_ratio_pct": round(want_ratio_pct, 2) if want_ratio_pct is not None else None,
        "retention_ratio_pct": round(retention_ratio_pct, 2) if retention_ratio_pct is not None else None,
        "popularity_index": round(popularity_index, 3),
        "weight_basis": round(total_w, 4),
        "raw_votes": {"have": have, "had": had, "want": want},
    }


# ---------------------------------------------------------------------------
# Group 8: main_accords  (scent vector -- NOT scored as quality)
# ---------------------------------------------------------------------------

def _main_accords(details: Any) -> dict[str, Any] | None:
    metrics = _get_card(details, "main accord")
    if not metrics:
        return None

    vector: list[dict[str, Any]] = []
    for m in metrics:
        label = str(m.get("label", "")).strip()
        if not label:
            continue
        try:
            score = float(m.get("pct") or 0.0)
        except (TypeError, ValueError):
            score = 0.0
        vector.append({"accord": label, "score": round(score, 2)})
    if not vector:
        return None

    vector.sort(key=lambda x: x["score"], reverse=True)
    top = [v["accord"] for v in vector[:7]]

    if len(top) == 1:
        accord_summary = f"A predominantly {top[0].lower()} fragrance."
    else:
        rest = top[1:]
        if len(rest) == 1:
            tail = rest[0].lower()
        else:
            tail = ", ".join(a.lower() for a in rest[:-1]) + f" and {rest[-1].lower()}"
        accord_summary = f"A {top[0].lower()} fragrance with {tail} facets."

    return {
        "scent_vector": vector,
        "top_accords": top,
        "accord_summary": accord_summary,
    }


# ---------------------------------------------------------------------------
# Group 9: notes  (preserved verbatim -- not scored yet)
# ---------------------------------------------------------------------------

def _notes(details: Any) -> dict[str, Any] | None:
    notes = getattr(details, "notes", None)
    if notes is None:
        return None
    top = list(getattr(notes, "top", []) or [])
    heart = list(getattr(notes, "heart", []) or [])
    base = list(getattr(notes, "base", []) or [])
    flat = list(getattr(notes, "flat", []) or [])
    if not (top or heart or base or flat):
        return None
    return {
        "has_pyramid": bool(getattr(notes, "has_pyramid", False)),
        "top": top,
        "heart": heart,
        "base": base,
        "flat": flat,
    }


# ---------------------------------------------------------------------------
# Group 1: headline  (computed last, from the component scores)
# ---------------------------------------------------------------------------

def _headline_copy(score: int, n_components: int) -> tuple[str, str]:
    if score >= 85:
        return (
            "Crowd Favorite",
            f"Overwhelmingly well received by the community, scoring {score}/100 "
            f"across {n_components} signal group(s).",
        )
    if score >= 70:
        return (
            "Well Liked",
            f"Solidly positive community reception at {score}/100, based on "
            f"{n_components} signal group(s).",
        )
    if score >= 55:
        return (
            "Generally Liked",
            f"Moderately positive overall, scoring {score}/100 across "
            f"{n_components} signal group(s).",
        )
    if score >= 40:
        return (
            "Mixed Reception",
            f"Community opinion is divided, landing at {score}/100 across "
            f"{n_components} signal group(s).",
        )
    return (
        "Polarizing",
        f"Largely underwhelming community reception at {score}/100, based on "
        f"{n_components} signal group(s).",
    )


def _headline(component_scores: dict[str, float | None]) -> dict[str, Any] | None:
    """crowd_consensus_score: weighted blend, weights renormalized over what exists."""
    parts = [
        (k, component_scores[k], w)
        for k, w in HEADLINE_WEIGHTS.items()
        if component_scores.get(k) is not None
    ]
    if not parts:
        return None
    total_w = sum(w for _, _, w in parts)
    score = sum(s * w for _, s, w in parts) / total_w
    score_int = int(round(score))
    label, summary = _headline_copy(score_int, len(parts))
    return {
        "crowd_consensus_score": score_int,
        "crowd_consensus_score_raw": score,
        "label": label,
        "summary": summary,
        "components_used": [k for k, _, _ in parts],
        "weight_basis": round(total_w, 4),
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def build_derived_metrics(details: Any) -> dict[str, Any]:
    """Consume a parsed `UnifiedDetails`-shaped object, emit normalized metrics.

    Every metric group is independent: a missing group is reported as ``None``
    and excluded from ``crowd_consensus_score`` (whose weights are renormalized
    over the groups that are present). ``source_coverage`` records which groups
    were available.
    """
    fg = _fg_rating_score(details)
    bn = _bn_sentiment_score(details)
    perf = _performance_score(details)
    val = _value_score(details)
    wear = _wear_profile(details)
    community = _community_interest_score(details)
    accords = _main_accords(details)
    notes = _notes(details)

    component_scores: dict[str, float | None] = {
        "fg_rating_score": fg["score_raw"] if fg else None,
        "bn_sentiment_score": bn["score_raw"] if bn else None,
        "performance_score": perf["score_raw"] if perf else None,
        "value_score": val["score_raw"] if val else None,
        "community_interest_score": community["score_raw"] if community else None,
    }
    headline = _headline(component_scores)

    source_coverage = {
        "fg_rating_score": fg is not None,
        "bn_sentiment_score": bn is not None,
        "performance_score": perf is not None,
        "value_score": val is not None,
        "wear_profile": wear is not None,
        "community_interest_score": community is not None,
        "main_accords": accords is not None,
        "notes": notes is not None,
    }

    return {
        "headline": headline,
        "fg_rating_score": fg,
        "bn_sentiment_score": bn,
        "performance_score": perf,
        "value_score": val,
        "wear_profile": wear,
        "community_interest_score": community,
        "main_accords": accords,
        "notes": notes,
        "source_coverage": source_coverage,
    }
