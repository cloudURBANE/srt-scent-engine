#!/usr/bin/env python3
"""Regression checks for enrichment resolver identity matching."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import fragrance_parser_full_rewrite_fixed as engine

UF = engine.UnifiedFragrance
CI = engine.CatalogItem

_FAILURES: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"  [{mark}] {label}" + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        _FAILURES.append(label)


def score(a_name: str, a_brand: str, b_name: str, b_brand: str) -> float:
    return engine.Orchestrator.identity_score(
        UF(name=a_name, brand=a_brand, year=""),
        CI(name=b_name, brand=b_brand, year="", url=""),
    )


def main() -> int:
    accept = engine.Orchestrator.CATALOG_ACCEPT
    print(f"Identity resolver checks (CATALOG_ACCEPT={accept})")

    ck_man = score("Calvin Klein Man", "Calvin Klein", "Man", "Calvin Klein")
    check("CK Man survives stopword-only token fallback", ck_man >= accept, f"score={ck_man:.4f}")

    ramz = score("Ramz Gold", "Lattafa", "Ramz Lattafa Gold", "Lattafa Perfumes")
    check("Lattafa Ramz Gold accepts generic brand suffix", ramz >= accept, f"score={ramz:.4f}")

    check(
        "Lattafa and Lattafa Perfumes are compatible",
        engine.IdentityTools.compatible_brand("Lattafa", "Lattafa Perfumes") is True,
    )
    check(
        "Lattafa and Lattafa Pride remain distinct",
        engine.IdentityTools.compatible_brand("Lattafa", "Lattafa Pride") is False,
    )
    check(
        "Lattafa and Armani remain incompatible",
        engine.IdentityTools.compatible_brand("Lattafa", "Armani") is False,
    )

    for other in ("CK Free", "Eternity For Men", "CK One"):
        other_score = score("Man", "Calvin Klein", other, "Calvin Klein")
        check(f"CK Man does not match {other}", other_score < accept, f"score={other_score:.4f}")

    sauvage = score("Sauvage", "Dior", "Sauvage", "Christian Dior")
    check("Dior Sauvage alias still matches Christian Dior", sauvage >= accept, f"score={sauvage:.4f}")

    if _FAILURES:
        print("\nFailures:")
        for label in _FAILURES:
            print(f"  - {label}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
