import json
import subprocess
import sys
from pathlib import Path

from scripts.db_audit_export import _annotate_cross_row_issues
from scripts.dirty_data_progress import compare_reports
from scripts.dirty_data import audit_row


def _report(rows):
    return {
        "total_rows": len(rows),
        "rows_with_issues": sum(bool(r["issues"]) for r in rows),
        "rows": rows,
    }


def test_compare_reports_tracks_resolved_persistent_and_new_rows():
    before = _report([
        {"source": "user_fragrances", "row_id": "1", "issues": "accord_case: ['amber']; family_unknown: None"},
        {"source": "global_fragrances", "row_id": "2", "issues": "gender_missing: None"},
    ])
    after = _report([
        {"source": "user_fragrances", "row_id": "1", "issues": "family_unknown: None"},
        {"source": "global_fragrances", "row_id": "3", "issues": "gender_missing: None"},
    ])

    result = compare_reports(before, after)

    assert result["resolved_issue_count"] == 2
    assert result["new_issue_count"] == 1
    assert result["categories"]["accord_case"]["resolved_rows"] == ["user_fragrances:1"]
    assert result["categories"]["family_unknown"]["persistent_rows"] == ["user_fragrances:1"]
    assert result["categories"]["gender_missing"]["new_rows"] == ["global_fragrances:3"]


def test_progress_cli_fail_on_new(tmp_path: Path):
    before = tmp_path / "before.json"
    after = tmp_path / "after.json"
    before.write_text(json.dumps(_report([])), encoding="utf-8")
    after.write_text(json.dumps(_report([
        {"source": "global_fragrances", "row_id": "3", "issues": "gender_missing: None"},
    ])), encoding="utf-8")

    run = subprocess.run(
        [sys.executable, "scripts/dirty_data_progress.py", str(before), str(after), "--fail-on-new"],
        cwd=Path(__file__).parent,
        capture_output=True,
        text=True,
        check=False,
    )

    assert run.returncode == 1
    assert '"new_issue_count": 1' in run.stdout


def test_identity_checks_are_report_only():
    assert "brand_missing" in audit_row({"name": "orphan"})
    assert "name_missing" in audit_row({"brand": "House"})
    assert "identity_mojibake" in audit_row({"brand": "HermÃ¨s", "name": "Test"})
    assert any(
        issue.startswith("source_url_malformed:")
        for issue in audit_row({"brand": "House", "name": "Test", "source_url": "not-a-url"})
    )


def test_duplicate_checks_are_scoped_per_owner_and_catalog():
    base = {
        "row_id": "1", "brand": "Creed", "name": "Aventus",
        "source_url": "https://example.test/aventus", "issues": "", "n_issues": 0,
    }
    rows = [
        {**base, "source": "user_fragrances", "owner": "owner-a"},
        {**base, "row_id": "2", "source": "user_fragrances", "owner": "owner-b"},
        {**base, "row_id": "3", "source": "global_fragrances", "owner": "global"},
        {**base, "row_id": "4", "source": "global_fragrances", "owner": "global"},
    ]

    _annotate_cross_row_issues(rows)

    assert rows[0]["issues"] == rows[1]["issues"] == ""
    assert "duplicate_identity" in rows[2]["issues"]
    assert "duplicate_source_url" in rows[3]["issues"]
