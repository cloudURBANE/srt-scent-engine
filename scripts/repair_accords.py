"""Repair derived_metrics blobs whose main accords were polluted by the
Fragrantica scrape leak (vote-count tokens like "14.9K" / love-hate words like
"Hate" sorted above the real accords and crowded them off the card -- the
"Dylan Blue only shows two accords" bug).

The blob is non-NULL, so the additive recompute sweep can never reach it. This
script rebuilds derived_metrics from the already-stored raw using the now-fixed
adapter (which drops junk accords) and force-overwrites only the rows that are
actually polluted. No network, no scrape -- it's a pure function of stored raw.

Dry-run by default (writes nothing); pass --commit to persist.

Run against the engine DB:
    railway run -s lively-adaptation -- python scripts/repair_accords.py            # dry-run
    railway run -s lively-adaptation -- python scripts/repair_accords.py --commit    # persist
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import api  # noqa: E402
import db  # noqa: E402
from derived_metrics_adapter import build_derived_metrics  # noqa: E402
from enrichment_facts import is_junk_accord_label, top_accords_from  # noqa: E402

PAGE = 500


def _stored_top_accords(record: dict) -> list[str]:
    """Raw, *unsanitized* top_accords from the stored blob (so we can detect junk)."""
    dm = record.get("derived_metrics")
    if not isinstance(dm, dict):
        return []
    main = dm.get("main_accords")
    if not isinstance(main, dict):
        return []
    top = main.get("top_accords")
    return [str(a).strip() for a in top if str(a or "").strip()] if isinstance(top, list) else []


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--commit", action="store_true", help="persist (default: dry-run)")
    ap.add_argument("--limit", type=int, default=None, help="cap rows scanned (debug)")
    args = ap.parse_args()

    db.init_db()
    if not db.ENABLED:
        print("DATABASE_URL not set / db disabled -- nothing to do.", file=sys.stderr)
        return 2

    total = db.count_fragrance_records()
    print(f"scanning {total} fragrance_records (commit={args.commit}) ...")

    scanned = polluted = repaired = no_raw = unchanged = errors = 0
    offset = 0
    while True:
        page = db.list_fragrance_records(limit=PAGE, offset=offset)
        if not page:
            break
        for record in page:
            if args.limit and scanned >= args.limit:
                break  # cap rows *scanned* (must precede the clean-row continue)
            scanned += 1
            stored = _stored_top_accords(record)
            if not any(is_junk_accord_label(a) for a in stored):
                continue  # clean (or empty) -- leave it alone
            polluted += 1
            key = record.get("record_key")
            label = f"{record.get('house')} | {record.get('name')}"
            if not api._record_has_detail_payload(record):  # noqa: SLF001
                no_raw += 1
                print(f"  [no-raw] {label}: polluted but raw is empty -- needs scrape")
                continue
            try:
                details = api._details_from_fragrance_record(record)  # noqa: SLF001
                dm = build_derived_metrics(details)
            except Exception as exc:  # noqa: BLE001
                errors += 1
                print(f"  [error] {label}: {exc}")
                continue
            clean = top_accords_from({"derived_metrics": dm}) if dm else []
            print(f"  [fix] {label}\n        before: {stored}\n        after:  {clean}")
            if args.commit:
                if db.set_record_derived_metrics(key, dm, force=True):
                    repaired += 1
                else:
                    unchanged += 1
        if args.limit and scanned >= args.limit:
            break
        offset += PAGE

    print(
        "\nsummary: "
        f"scanned={scanned} polluted={polluted} repaired={repaired} "
        f"no_raw={no_raw} unchanged={unchanged} errors={errors} "
        f"committed={args.commit}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
