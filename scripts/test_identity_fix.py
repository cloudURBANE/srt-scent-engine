#!/usr/bin/env python3
"""Regression checks for enrichment resolver identity matching."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import fragrance_parser_full_rewrite_fixed as engine
import scripts.enrichment_worker as worker

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


def parfumo_score(
    a_name: str,
    a_brand: str,
    b_name: str,
    b_brand: str | None = None,
    year: str = "",
) -> float:
    return engine.ParfumoEngine.score_record(
        a_brand,
        a_name,
        year,
        engine.ParfumoRecord(
            url="https://www.parfumo.com/Perfumes/_test_/identity",
            name=b_name,
            house=b_brand or a_brand,
            year=year,
        ),
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

    hermes_urls = {
        "https://www.fragrantica.com/perfume/Hermes/Twilly-d-Hermes-46145.html": "Twilly d'Hermes",
        "https://www.fragrantica.com/perfume/Hermes/Galop-d-Hermes-39584.html": "Galop d'Hermes",
        "https://www.fragrantica.com/perfume/Hermes/Terre-d-Hermes-Eau-Givree-72439.html": "Terre d'Hermes Eau Givree",
    }
    for url, expected in hermes_urls.items():
        derived = engine.FragranticaEngine.name_from_url(url)
        check(
            f"Fragrantica URL identity restores {expected}",
            derived == expected,
            f"derived={derived!r}",
        )

    repaired = worker._specific_identity(
        UF(
            name="Terre Dhermes Eau Givree",
            brand="Hermes",
            year="",
            bn_url="",
            frag_url="https://www.fragrantica.com/perfume/Hermes/Terre-d-Hermes-Eau-Givree-72439.html",
        )
    )
    check(
        "worker completion identity repairs glued Hermes elision",
        repaired["name"] == "Terre d'Hermes Eau Givree",
        f"name={repaired['name']!r}",
    )

    ck_parfumo = parfumo_score("Man", "Calvin Klein", "Calvin Klein Man (Eau de Toilette)")
    check(
        "Parfumo concentration suffix does not block CK Man",
        ck_parfumo >= engine.ParfumoEngine.ACCEPT,
        f"score={ck_parfumo:.4f}",
    )

    miami = parfumo_score(
        "Miami Poolside",
        "Clive Christian",
        "The Art Of Travel Collection - Miami Poolside",
        "Clive Christian",
        "2019",
    )
    check(
        "Parfumo collection prefix does not block Miami Poolside",
        miami >= engine.ParfumoEngine.ACCEPT,
        f"score={miami:.4f}",
    )

    ranked = engine.ParfumoEngine._rank_discovered_urls(
        [
            "https://www.parfumo.com/Perfumes/Clive_Christian/xxi-art-deco-blonde-amber",
            "https://www.parfumo.com/Perfumes/Clive_Christian/1872-masculine",
            "https://www.parfumo.com/Perfumes/Clive_Christian/the-art-of-travel-collection-miami-poolside",
        ],
        "Clive Christian",
        "Miami Poolside",
    )
    check(
        "Parfumo brand-crawl ranks late Miami slug before unrelated house URLs",
        ranked[0].endswith("/the-art-of-travel-collection-miami-poolside"),
    )

    saved_env = {
        "PARFUMO_FALLBACK_ENABLED": os.environ.get("PARFUMO_FALLBACK_ENABLED"),
        "PARFUMO_BRAND_CRAWL_ENABLED": os.environ.get("PARFUMO_BRAND_CRAWL_ENABLED"),
        "SERPER_API_KEY": os.environ.get("SERPER_API_KEY"),
    }
    try:
        os.environ["PARFUMO_FALLBACK_ENABLED"] = "1"
        os.environ.pop("PARFUMO_BRAND_CRAWL_ENABLED", None)
        os.environ.pop("SERPER_API_KEY", None)
        check(
            "Parfumo brand crawl defaults on when fallback is enabled and Serper is missing",
            engine.ParfumoEngine.brand_crawl_enabled() is True,
        )
        os.environ["PARFUMO_BRAND_CRAWL_ENABLED"] = "0"
        check(
            "Explicit Parfumo brand-crawl disable is respected",
            engine.ParfumoEngine.brand_crawl_enabled() is False,
        )
    finally:
        for key, value in saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    if _FAILURES:
        print("\nFailures:")
        for label in _FAILURES:
            print(f"  - {label}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
