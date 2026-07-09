#!/usr/bin/env python3
"""Broad, offline-first audit of Main-accords leaks across every fragrance we have data for.

The "Main accords" card container over-captures adjacent Fragrantica page text; past
leaks were vote counts ("14.9K"), rating buckets ("Hate"), season buckets ("Summer"),
shop/ad copy ("Online shops offers"), meta headings ("Fragrance Composition") and the
fragrance's OWN name ("BAL A VERSAILLES"). :func:`enrichment_facts.is_junk_accord_label`
is the single authority that classifies all of these.

This audit loads every real fragrance data point the repo can reach WITHOUT external
credentials and scans each one's accord labels (from ``frag_cards['Main accords']`` and
from any stored ``derived_metrics.main_accords``) through that predicate -- WITH the
fragrance's own name/brand identity so the self-name class is exercised too. Findings are
grouped by leak type and quantified by distinct fragrances affected.

Sources (each optional; missing ones are skipped, never fatal):
  * fg_cache/fg_detail_cache_v1.json   -- post-parse Fragrantica detail captures
  * fg_cache/parfinity_cache.json      -- Parfinity fallback captures
  * production_verification_results.json / decodo_scraper_*.json -- fixtures
  * ENGINE_DATABASE_URL or DATABASE_URL -> fragrance_records.derived_metrics_json (read-only)
  * DATABASE_URL -> user_fragrances.fragrance_data / global_fragrances.profile_data (read-only)

This script is READ-ONLY. It never writes. Use scripts/backfill_selfname_accords.py to
repair anything it finds.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from collections import Counter, defaultdict
from typing import Any

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import enrichment_facts as ef


def _classify(label: str, *, name: str = "", brand: str = "") -> str | None:
    """Return the leak category for a junk label, or None when it is a real accord.

    Categories mirror the rule blocks inside is_junk_accord_label so the audit can
    attribute each leak to the mechanism that catches it.
    """
    s = str(label or "").strip()
    if not s:
        return "empty"
    # Fast path: not junk under any rule (label-only) and not a self-name leak.
    if not ef.is_junk_accord_label(s, fragrance_name=name, brand=brand):
        return None
    low = s.lower()
    norm = ef._normalize_accord_identity(s)
    if ef._VOTE_COUNT_RE.match(s):
        return "vote_count"
    if low in ef._RATING_WORD_LABELS:
        return "rating_word"
    if low in ef._SEASON_TIME_LABELS:
        return "season_time"
    if low in ef._AD_TEXT_LABELS or any(m in low for m in ef._AD_TEXT_MARKERS):
        return "ad_text"
    if norm in ef._META_STRUCTURAL_LABELS:
        return "meta_structural"
    # Reached only when the label-only predicate said "clean" but identity flipped it.
    if norm and norm not in ef._REAL_ACCORD_LABELS:
        for ident in (name, brand):
            if ident and ef._normalize_accord_identity(ident) == norm:
                return "self_name"
    return "other"


def _accord_labels_from_frag_cards(frag_cards: Any) -> list[str]:
    out: list[str] = []
    if not isinstance(frag_cards, dict):
        return out
    for card_name, rows in frag_cards.items():
        if "main accord" not in str(card_name).lower() or not isinstance(rows, list):
            continue
        for row in rows:
            if isinstance(row, dict):
                lbl = row.get("label") or row.get("display") or row.get("accord") or row.get("name")
            else:
                lbl = row
            if lbl:
                out.append(str(lbl))
    return out


def _accord_labels_from_derived(dm: Any) -> list[str]:
    out: list[str] = []
    if not isinstance(dm, dict):
        return out
    main = dm.get("main_accords") if isinstance(dm.get("main_accords"), dict) else None
    if not isinstance(main, dict):
        return out
    top = main.get("top_accords")
    if isinstance(top, list):
        out.extend(str(t) for t in top if t)
    vector = main.get("scent_vector")
    if isinstance(vector, list):
        out.extend(str(item.get("accord")) for item in vector if isinstance(item, dict) and item.get("accord"))
    return out


class Audit:
    def __init__(self) -> None:
        self.fragrances = 0
        self.with_accords = 0
        self.labels = 0
        self.cat_labels: Counter[str] = Counter()
        self.cat_frags: dict[str, set[str]] = defaultdict(set)
        self.examples: dict[str, list[str]] = defaultdict(list)

    def add(self, source: str, ident: str, name: str, brand: str, labels: list[str]) -> None:
        self.fragrances += 1
        if labels:
            self.with_accords += 1
        for lbl in labels:
            self.labels += 1
            cat = _classify(lbl, name=name, brand=brand)
            if cat is None:
                continue
            self.cat_labels[cat] += 1
            self.cat_frags[cat].add(f"{source}|{ident}")
            if len(self.examples[cat]) < 12:
                self.examples[cat].append(f"[{source}] {brand} / {name}: {lbl!r}")

    def report(self) -> int:
        print("=" * 72)
        print("MAIN-ACCORDS LEAK AUDIT")
        print("=" * 72)
        print(f"fragrances scanned : {self.fragrances}")
        print(f"  with accord card : {self.with_accords}")
        print(f"accord labels seen : {self.labels}")
        total_junk = sum(self.cat_labels.values())
        print(f"junk labels found  : {total_junk}")
        print("-" * 72)
        if not total_junk:
            print("NO leaks under the hardened predicate across all local data points.")
        else:
            for cat in sorted(self.cat_labels, key=lambda c: -self.cat_labels[c]):
                print(f"  {cat:16s} labels={self.cat_labels[cat]:5d}  distinct_frags={len(self.cat_frags[cat])}")
            print("-" * 72)
            print("examples:")
            for cat, exs in self.examples.items():
                for e in exs:
                    print(f"  [{cat}] {e}")
        print("=" * 72)
        # Non-zero exit only when a self-name / meta leak (the classes this hardening
        # targets) is still present -- useful as a CI gate.
        targeted = self.cat_labels.get("self_name", 0) + self.cat_labels.get("meta_structural", 0)
        return 1 if targeted else 0


def _load_json(path: str) -> Any:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def audit_local(audit: Audit) -> None:
    # 1. Fragrantica detail cache
    detail = _load_json(os.path.join(ROOT, "fg_cache", "fg_detail_cache_v1.json"))
    entries = (detail or {}).get("entries") if isinstance(detail, dict) else None
    for key, v in (entries or {}).items():
        if not isinstance(v, dict):
            continue
        labels = _accord_labels_from_frag_cards(v.get("frag_cards"))
        labels += _accord_labels_from_derived(v.get("derived_metrics"))
        audit.add("fg_detail_cache", str(key), v.get("name", "") or "", v.get("house", "") or "", labels)

    # 2. Parfinity cache
    parf = _load_json(os.path.join(ROOT, "fg_cache", "parfinity_cache.json"))
    pentries = (parf or {}).get("entries") if isinstance(parf, dict) else None
    for key, v in (pentries or {}).items():
        if not isinstance(v, dict):
            continue
        labels = _accord_labels_from_frag_cards(v.get("frag_cards"))
        labels += _accord_labels_from_derived(v.get("derived_metrics"))
        audit.add("parfinity_cache", str(key), v.get("name", "") or "", v.get("house", "") or "", labels)

    # 3. Loose JSON fixtures that may embed detail bodies with accord cards
    for path in glob.glob(os.path.join(ROOT, "*.json")):
        data = _load_json(path)
        base = os.path.basename(path)
        # production_verification_results.json -> results[].details
        results = (data or {}).get("results") if isinstance(data, dict) else None
        for item in results or []:
            det = item.get("details") if isinstance(item, dict) else None
            if isinstance(det, dict):
                labels = _accord_labels_from_frag_cards(det.get("frag_cards"))
                labels += _accord_labels_from_derived(det.get("derived_metrics"))
                if labels:
                    audit.add(base, str(item.get("name", "")), det.get("name", "") or item.get("name", "") or "",
                              det.get("house", "") or det.get("brand", "") or "", labels)


def audit_engine_db(audit: Audit) -> None:
    dsn = os.environ.get("ENGINE_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not dsn:
        print("[engine-db] no ENGINE_DATABASE_URL/DATABASE_URL -> skipped")
        return
    try:
        import psycopg
    except ImportError:
        print("[engine-db] psycopg not installed -> skipped")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT record_key, house, name, derived_metrics_json FROM fragrance_records "
                "WHERE derived_metrics_json IS NOT NULL"
            )
            n = 0
            for record_key, house, name, dm in cur.fetchall():
                labels = _accord_labels_from_derived(dm)
                audit.add("engine_db", str(record_key), name or "", house or "", labels)
                n += 1
            print(f"[engine-db] scanned {n} fragrance_records rows with derived_metrics_json")
    except Exception as exc:  # noqa: BLE001
        print(f"[engine-db] skipped ({exc})")


def audit_wardrobe_db(audit: Audit) -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("[wardrobe-db] no DATABASE_URL -> skipped")
        return
    try:
        import psycopg
    except ImportError:
        print("[wardrobe-db] psycopg not installed -> skipped")
        return
    for table, col in (("user_fragrances", "fragrance_data"), ("global_fragrances", "profile_data")):
        try:
            with psycopg.connect(dsn) as conn, conn.cursor() as cur:
                cur.execute(f"SELECT id, {col} FROM {table}")
                n = 0
                for rid, blob in cur.fetchall():
                    if not isinstance(blob, dict):
                        continue
                    name = blob.get("name", "") or ""
                    brand = blob.get("brand", "") or blob.get("house", "") or ""
                    labels = _accord_labels_from_derived(blob.get("derived_metrics"))
                    labels += _accord_labels_from_frag_cards(blob.get("frag_cards"))
                    audit.add(table, str(rid), name, brand, labels)
                    n += 1
                print(f"[wardrobe-db] scanned {n} {table} rows")
        except Exception as exc:  # noqa: BLE001
            print(f"[wardrobe-db] {table} skipped ({exc})")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--engine-db", action="store_true", help="also scan fragrance_records.derived_metrics_json")
    ap.add_argument("--wardrobe-db", action="store_true", help="also scan user_fragrances/global_fragrances")
    args = ap.parse_args()

    audit = Audit()
    audit_local(audit)
    if args.engine_db:
        audit_engine_db(audit)
    if args.wardrobe_db:
        audit_wardrobe_db(audit)
    return audit.report()


if __name__ == "__main__":
    raise SystemExit(main())
