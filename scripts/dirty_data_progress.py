#!/usr/bin/env python3
"""Compare two dirty-data audit reports without touching the database.

The output identifies resolved, persistent, and newly introduced row/category
flags. Exit status is non-zero with ``--fail-on-new`` when any new issue appears,
which makes the report suitable as a cleanup regression gate.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _issue_rows(report: dict[str, Any]) -> dict[str, set[str]]:
    indexed = report.get("issue_rows")
    if isinstance(indexed, dict):
        return {str(tag): {str(key) for key in keys} for tag, keys in indexed.items()}

    out: dict[str, set[str]] = {}
    for row in report.get("rows", []):
        key = f"{row.get('source')}:{row.get('row_id')}"
        for flag in str(row.get("issues") or "").split("; "):
            if not flag:
                continue
            out.setdefault(flag.split(":", 1)[0], set()).add(key)
    return out


def compare_reports(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    old = _issue_rows(before)
    new = _issue_rows(after)
    categories: dict[str, Any] = {}
    for tag in sorted(old.keys() | new.keys()):
        old_rows = old.get(tag, set())
        new_rows = new.get(tag, set())
        categories[tag] = {
            "before": len(old_rows),
            "after": len(new_rows),
            "delta": len(new_rows) - len(old_rows),
            "resolved_rows": sorted(old_rows - new_rows),
            "new_rows": sorted(new_rows - old_rows),
            "persistent_rows": sorted(old_rows & new_rows),
        }
    return {
        "before_snapshot": before.get("snapshot_id"),
        "after_snapshot": after.get("snapshot_id"),
        "before_total_rows": before.get("total_rows"),
        "after_total_rows": after.get("total_rows"),
        "before_rows_with_issues": before.get("rows_with_issues"),
        "after_rows_with_issues": after.get("rows_with_issues"),
        "new_issue_count": sum(len(v["new_rows"]) for v in categories.values()),
        "resolved_issue_count": sum(len(v["resolved_rows"]) for v in categories.values()),
        "categories": categories,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("before")
    parser.add_argument("after")
    parser.add_argument("--out", help="optional JSON output path")
    parser.add_argument("--fail-on-new", action="store_true")
    args = parser.parse_args()

    before = json.loads(Path(args.before).read_text(encoding="utf-8"))
    after = json.loads(Path(args.after).read_text(encoding="utf-8"))
    result = compare_reports(before, after)
    rendered = json.dumps(result, indent=2)
    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 1 if args.fail_on_new and result["new_issue_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
