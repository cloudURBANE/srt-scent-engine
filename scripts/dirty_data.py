#!/usr/bin/env python3
"""Shared dirty-data taxonomy, detection (audit) and safe normalization (fix).

Single source of truth used by both:
  * ``scripts/db_audit_export.py``        -- read-only: flags issues for the report.
  * ``scripts/sanitize_wardrobe_accords.py`` -- write path: applies the *safe*
    subset of fixes so the data self-corrects on every sweep (forward-focused).

"Safe" normalizations only change presentation, never semantics, and are
idempotent -- running them twice is a no-op:
  * accord / family **casing** -> canonical Title Case ("woody" -> "Woody")
  * the ``Chypere`` -> ``Chypre`` spelling typo
  * numeric ``year`` stored as a string -> ``int``
  * junk accord tokens (digits / %, "sponsored", URLs, "ml") dropped

Deliberately NOT auto-fixed (flagged only -- need a source decision, would be a
semantic change): deprecated ``Oriental`` family, ``Unknown`` family/conc,
missing gender, apostrophe-stripped names, year embedded in the name. Those go in
the report for a human to resolve.
"""
from __future__ import annotations

import datetime as _dt
import re
from typing import Any

VALID_CONCENTRATIONS = {
    "Eau de Cologne", "Eau Fraiche", "Eau de Toilette", "Eau de Parfum",
    "Parfum", "Extrait", "Elixir", "Aftershave", "Body Spray", "Body Mist",
    "Perfume Oil", "Soie de Parfum",
}
VALID_GENDERS = {"Masculine", "Feminine", "Unisex"}

# Base olfactive family words (Fragrantica taxonomy). Compound families
# ("Amber Woody", "Aromatic Fougere") are valid when every word is in this set.
_FAMILY_WORDS = {
    "Floral", "Amber", "Woody", "Fresh", "Aromatic", "Citrus", "Chypre",
    "Fougere", "Gourmand", "Leather", "Aquatic", "Green", "Fruity", "Spicy",
    "Musk", "Powdery", "Sweet", "Animalic", "Smoky", "Balsamic", "Mossy",
    "Conifer", "Marine", "Earthy", "White",
    # legacy / still-emitted family words (flagged separately as deprecated)
    "Oriental",
}
_BLANK = {"", "unknown", "unknown family", "none", "null", "n/a", "-", "universal"}
_DEPRECATED_FAMILY = {"oriental"}
_FAMILY_TYPO = {"chypere": "Chypre"}

# A token is junk (not a real accord) if it carries any of these.
_ACCORD_JUNK_RE = re.compile(
    r"\d|%|\bml\b|\bsponsor|\bhttp|\$|\bhate\b|\blove it\b|\bclick\b|\bad\b",
    re.I,
)


def _is_blank(v: Any) -> bool:
    return v is None or str(v).strip().lower() in _BLANK


# --------------------------------------------------------------------------- #
# Normalizers (safe, idempotent)
# --------------------------------------------------------------------------- #
def norm_accord(token: str) -> str:
    """Canonical Title-Case accord. 'warm  spicy' -> 'Warm Spicy'."""
    return re.sub(r"\s+", " ", str(token).strip()).title()


def is_junk_accord(token: str) -> bool:
    t = str(token).strip()
    return (not t) or bool(_ACCORD_JUNK_RE.search(t)) or len(t) > 24


def norm_family(value: Any) -> Any:
    """Title-case a family + fix the Chypere typo. Leaves blanks/None as-is.

    Does NOT remap deprecated 'Oriental' (semantic change -> flagged only)."""
    if _is_blank(value):
        return value
    words = re.sub(r"\s+", " ", str(value).strip()).split(" ")
    fixed = [_FAMILY_TYPO.get(w.lower(), w.capitalize()) for w in words]
    return " ".join(fixed)


def norm_year(value: Any) -> Any:
    """Numeric-string year -> int. Leaves everything else untouched."""
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return value


def normalize_row(blob: dict[str, Any]) -> bool:
    """Apply the SAFE fix subset in place. Returns whether anything changed."""
    if not isinstance(blob, dict):
        return False
    changed = False

    acc = blob.get("accords")
    if isinstance(acc, list):
        out: list[str] = []
        seen: set[str] = set()
        for a in acc:
            if is_junk_accord(a):
                continue
            na = norm_accord(a)
            if na and na.lower() not in seen:
                seen.add(na.lower())
                out.append(na)
        if out != [str(a) for a in acc]:
            blob["accords"] = out
            changed = True

    fam = blob.get("family")
    nf = norm_family(fam)
    if nf != fam:
        blob["family"] = nf
        changed = True

    yr = blob.get("year")
    ny = norm_year(yr)
    if ny != yr:
        blob["year"] = ny
        changed = True

    return changed


# --------------------------------------------------------------------------- #
# Auditor (read-only detection) -- returns list of "tag: detail" flags
# --------------------------------------------------------------------------- #
def audit_row(blob: dict[str, Any]) -> list[str]:
    if not isinstance(blob, dict):
        return ["bad_blob: not an object"]
    issues: list[str] = []
    name = str(blob.get("name") or "").strip()
    brand = str(blob.get("brand") or blob.get("house") or "").strip()

    # --- accords ---
    acc = blob.get("accords")
    if isinstance(acc, list):
        junk = [a for a in acc if is_junk_accord(a)]
        if junk:
            issues.append(f"accord_junk: {junk}")
        lower = [a for a in acc if isinstance(a, str) and a and a != norm_accord(a) and a not in junk]
        if lower:
            issues.append(f"accord_case: {lower}")
        if len(acc) == 0:
            issues.append("accords_empty")
        elif len([a for a in acc if not is_junk_accord(a)]) == 1:
            issues.append(f"single_accord: {acc}")
    elif acc is None:
        issues.append("accords_missing")

    # --- family ---
    fam = blob.get("family")
    if _is_blank(fam):
        issues.append(f"family_unknown: {fam!r}")
    else:
        fl = str(fam).strip().lower()
        if fl in _DEPRECATED_FAMILY:
            issues.append(f"family_deprecated: {fam!r} (Oriental->Amber)")
        elif fl in _FAMILY_TYPO:
            issues.append(f"family_typo: {fam!r}")
        elif str(fam) != norm_family(fam):
            issues.append(f"family_case: {fam!r}")
        # unexpected family word?
        for w in re.split(r"\s+", str(fam).strip()):
            if w and w.capitalize() not in _FAMILY_WORDS:
                issues.append(f"family_unexpected: {fam!r}")
                break

    # --- concentration ---
    conc = blob.get("concentration")
    if _is_blank(conc):
        issues.append(f"concentration_unknown: {conc!r}")
    elif str(conc).strip() not in VALID_CONCENTRATIONS:
        issues.append(f"concentration_unexpected: {conc!r}")

    # --- gender ---
    gen = blob.get("gender")
    if _is_blank(gen):
        issues.append(f"gender_missing: {gen!r}")
    elif str(gen).strip() not in VALID_GENDERS:
        issues.append(f"gender_unexpected: {gen!r}")

    # --- year ---
    yr = blob.get("year")
    if isinstance(yr, str) and yr.strip().isdigit():
        issues.append(f"year_string: {yr!r}")
        yv = int(yr)
    elif isinstance(yr, int):
        yv = yr
    else:
        yv = None
    if yv is not None:
        nxt = _dt.date.today().year + 1
        if yv < 1800 or yv > nxt:
            issues.append(f"year_range: {yv}")

    # --- name / brand hygiene ---
    if name and brand and name.lower() == brand.lower():
        issues.append("name_equals_brand")
    if "|" in name and len({p.strip().lower() for p in name.split("|") if p.strip()}) == 1:
        issues.append(f"name_self_dup: {name!r}")
    if re.search(r"\b(19|20)\d{2}\b", name):
        issues.append(f"name_has_year: {name!r}")

    return issues
