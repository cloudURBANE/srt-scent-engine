#!/usr/bin/env python3
"""
test_fragrantica_description_year.py

Regression guard for the Fragrantica launch-year gap.

The Fragrantica detail parser used to extract notes/accords/ratings/gender but
never the descriptive paragraph ("<name> was launched in <year>"). Since
heal_missing_gender_and_year recovers the launch year ONLY from
details.description, Fragrantica-sourced fragrances (e.g. Dylan Blue, and the
~488 catalog rows with a fg_url but no year) could never heal their year.

These checks lock in that the description is now captured and that the year
heals from it. Runs as a plain script (no pytest required):

    python test_fragrantica_description_year.py

Exit code is non-zero if any check fails.
"""
import sys

from bs4 import BeautifulSoup

import fragrance_parser_full_rewrite_fixed as engine


def _check(name: str, cond: bool) -> bool:
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
    return cond


def main() -> int:
    ok = True

    # 1. Schema.org itemprop="description" is the canonical Fragrantica markup,
    #    and a later review block must NOT win over the real description.
    html = (
        '<html><body>'
        '<div itemprop="description"><p>Versace Pour Homme Dylan Blue by Versace '
        'is a fragrance for men. Versace Pour Homme Dylan Blue was launched in '
        '2016.</p></div>'
        '<div id="fragrev">a review that mentions launched in 1999</div>'
        '</body></html>'
    )
    desc = engine.FragranticaEngine.extract_description(BeautifulSoup(html, "html.parser"))
    ok &= _check("itemprop description extracted", "launched in 2016" in desc.lower())

    # 2. Markup-drift fallback: no itemprop, recover the launch paragraph anyway.
    html2 = (
        "<html><body><p>Intro line.</p>"
        "<p>Ramz Lattafa Gold was launched in 2019 by Lattafa.</p></body></html>"
    )
    desc2 = engine.FragranticaEngine.extract_description(BeautifulSoup(html2, "html.parser"))
    ok &= _check("fallback recovers launch paragraph", "launched in 2019" in desc2.lower())

    # 3. Full chain: a captured description heals an empty year.
    details = engine.UnifiedDetails(notes=engine.NotesList())
    details.description = desc
    selected = engine.UnifiedFragrance(name="Dylan Blue", brand="Versace", year="")
    engine.heal_missing_gender_and_year(selected, details)
    ok &= _check("year heals to 2016 from description", str(selected.year) == "2016")

    # 4. Empty page yields "" (no crash, no false positive).
    desc3 = engine.FragranticaEngine.extract_description(
        BeautifulSoup("<html><body><p>nothing here</p></body></html>", "html.parser")
    )
    ok &= _check("empty page returns blank", desc3 == "")

    print("ALL CHECKS PASSED" if ok else "CHECKS FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
