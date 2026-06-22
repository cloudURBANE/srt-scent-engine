#!/usr/bin/env python3
"""
test_fragrantica_image_harvest.py

Regression guard for the "no image after a successful add" gap.

The user-facing /search and /details paths do no image discovery — the search
id token does not encode an image, so a cold, never-enriched fragrance returned
image_url=None and rendered "no image" in the app until the offline worker ran.
FragranticaEngine now harvests the bottle image from the detail page's durable
structured sources (og:image, then JSON-LD image) at parse time, and
_details_to_dict falls back to it when the resolved candidate has none.

These checks lock in that harvest. Runs as a plain script (no pytest required):

    python test_fragrantica_image_harvest.py

Exit code is non-zero if any check fails.
"""
import sys

from bs4 import BeautifulSoup

import fragrance_parser_full_rewrite_fixed as engine

OG = "https://fimgs.net/mdimg/perfume/375x500.bottle.jpg"
LD = "https://fimgs.net/mdimg/perfume/ld.jpg"


def _check(name: str, cond: bool) -> bool:
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
    return cond


def main() -> int:
    ok = True
    extract = engine.FragranticaEngine.extract_image_url

    # 1. og:image is the canonical share image FG sets to the bottle packshot.
    html = f'<html><head><meta property="og:image" content="{OG}"></head></html>'
    ok &= _check("og:image harvested", extract(BeautifulSoup(html, "html.parser")) == OG)

    # 2. JSON-LD `image` string fallback when og:image is absent.
    html2 = f'<html><head><script type="application/ld+json">{{"image":"{LD}"}}</script></head></html>'
    ok &= _check("JSON-LD image string fallback", extract(BeautifulSoup(html2, "html.parser")) == LD)

    # 3. JSON-LD ImageObject list fallback.
    html3 = f'<html><head><script type="application/ld+json">{{"image":[{{"url":"{LD}"}}]}}</script></head></html>'
    ok &= _check("JSON-LD ImageObject list fallback", extract(BeautifulSoup(html3, "html.parser")) == LD)

    # 4. Relative / non-http values are rejected (never a false image).
    html4 = '<html><head><meta property="og:image" content="/relative.jpg"></head></html>'
    ok &= _check("relative og:image rejected", extract(BeautifulSoup(html4, "html.parser")) == "")

    # 5. Empty page yields "" (no crash, no false positive).
    ok &= _check("empty page returns blank", extract(BeautifulSoup("<html></html>", "html.parser")) == "")

    # 6. UnifiedDetails carries the harvested image field (default empty).
    details = engine.UnifiedDetails(notes=engine.NotesList())
    ok &= _check("UnifiedDetails.image_url defaults empty", details.image_url == "")
    details.image_url = OG
    ok &= _check("UnifiedDetails.image_url is settable", details.image_url == OG)

    print("ALL CHECKS PASSED" if ok else "CHECKS FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
