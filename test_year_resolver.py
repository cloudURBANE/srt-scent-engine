from year_resolver import (
    YearEvidence,
    choose_year,
    extract_year_evidence,
    extract_year_unknown_signal,
    resolve_year_decodo,
)

# Verbatim rows from a LIVE Decodo SERP for '"Lattafa Ramz Gold" release date'
# (saved 2026-06-17). The canonical databases name it "Ramz Lattafa Gold" with
# the brand interleaved, and Parfumo explicitly prints that the year is unknown;
# no source states a launch year. This is the real evidence behind the decision
# to NOT fabricate a year for Ramz Gold.
_LIVE_RAMZ_GOLD_ROWS = [
    {
        "title": "Lattafa Ramz Gold Eau De Parfum 3.4 Oz Unisex ...",
        "snippet": "Buy Lattafa Ramz Gold Eau De Parfum 3.4 Oz Unisex Fragrance Lattafa at Walmart.com.",
        "url_raw": "https://www.walmart.com/ip/whatever",
        "domain": "walmart.com",
    },
    {
        "title": "Ramz Lattafa (Gold) Lattafa Perfumes - Fragrantica.com",
        "snippet": "Ramz Lattafa (Gold) by Lattafa Perfumes is a fragrance for women and men. Top notes are Apple, Peach, Pear, Black Currant and Sweet Orange.",
        "url_raw": "https://www.fragrantica.com/perfume/Lattafa-Perfumes/Ramz-Lattafa-Gold-12345.html",
        "domain": "fragrantica.com",
    },
    {
        "title": "Ramz Lattafa Gold by Lattafa » Reviews & Perfume Facts",
        "snippet": "Mar 10, 2026 · A perfume by Lattafa for women. The release year is unknown. The scent is fruity-sweet. It is being marketed by Lattafa Perfumes Industries LLC. Compare",
        "url_raw": "https://www.parfumo.com/Parfums/Lattafa/ramz-lattafa-gold",
        "domain": "parfumo.com",
    },
]


def test_live_ramz_gold_yields_no_fabricated_year():
    """Against the real SERP, no explicit launch year exists, so nothing is invented."""
    evidence = extract_year_evidence(_LIVE_RAMZ_GOLD_ROWS, "Lattafa", "Ramz Gold", now_year=2026)
    assert evidence == []
    assert choose_year(evidence) is None


def test_live_ramz_gold_detects_authoritative_year_unknown():
    """Parfumo's explicit 'release year is unknown' is captured as a durable
    source-unsuppliable signal, even though the page names it 'Ramz Lattafa Gold'."""
    signal = extract_year_unknown_signal(_LIVE_RAMZ_GOLD_ROWS, "Lattafa", "Ramz Gold")
    assert signal is not None
    assert signal["year"] is None
    assert signal["unresolvable"] is True
    assert signal["year_meta"]["source"] == "decodo_serp_authoritative_unknown"
    assert signal["year_meta"]["domains"] == ["parfumo.com"]


def test_year_unknown_signal_ignores_siblings_and_retailers():
    # A sibling the page does not name is never matched ("silver" token absent).
    assert extract_year_unknown_signal(_LIVE_RAMZ_GOLD_ROWS, "Lattafa", "Ramz Silver") is None
    # A non-authoritative retailer saying "unknown" is not trusted.
    retailer = [{
        "title": "Some Cologne",
        "snippet": "Some Cologne by Brand X. The release year is unknown.",
        "url_raw": "https://shop.test/some-cologne",
        "domain": "shop.test",
    }]
    assert extract_year_unknown_signal(retailer, "Brand X", "Some Cologne") is None


def test_resolve_returns_authoritative_unknown_when_no_year_found():
    class Client:
        @classmethod
        def enabled(cls):
            return True

        @classmethod
        def _post_google(cls, query, timeout):
            return {"organic": [
                {"title": "Lattafa Ramz Gold EDP", "description": "Buy Ramz Gold at retailer.", "link": "https://shop.test/gold"}
            ]}

        @classmethod
        def _post_bing(cls, query, timeout):
            return {"organic": [{
                "title": "Ramz Lattafa Gold by Lattafa » Reviews & Perfume Facts",
                "snippet": "A perfume by Lattafa for women. The release year is unknown.",
                "url": "https://www.parfumo.com/Parfums/Lattafa/ramz-lattafa-gold",
            }]}

        @staticmethod
        def _search_entries(payload):
            return payload["organic"]

        @staticmethod
        def _entry_link(entry):
            return entry.get("link") or entry.get("url") or ""

        @staticmethod
        def _log_request_failure(kind, exc):
            raise AssertionError((kind, exc))

    result = resolve_year_decodo("Lattafa", "Ramz Gold", client=Client)
    assert result is not None
    assert result["year"] is None
    assert result["unresolvable"] is True
    assert result["year_meta"]["source"] == "decodo_serp_authoritative_unknown"


def test_extracts_supplied_ramz_gold_release_statement():
    rows = [{
        "title": "Release Date of Lattafa Ramz Gold",
        "snippet": "Lattafa Ramz Gold was released in 2021 as part of Lattafa Perfumes' fragrance lineup.",
        "url_raw": "https://www.rdspabeautyshop.com/products/ramz-lattafa",
    }]
    evidence = extract_year_evidence(rows, "Lattafa", "Ramz Gold", now_year=2026)
    assert [(item.year, item.domain) for item in evidence] == [(2021, "rdspabeautyshop.com")]
    resolved = choose_year(evidence)
    assert resolved["year"] == "2021"
    assert resolved["year_meta"]["confidence"] == 95


def test_rejects_bare_years_wrong_products_and_conflicts():
    rows = [
        {"title": "Ramz Gold", "snippet": "Copyright 2021. Ramz Gold has fruity notes.", "url_raw": "https://shop.test/gold"},
        {"title": "Ramz Gold", "snippet": "Lattafa Ramz Silver was released in 2021.", "url_raw": "https://one.test/silver"},
        {"title": "Sauvage", "snippet": "Dior Sauvage Elixir was released in 2021.", "url_raw": "https://one.test/elixir"},
        {"title": "Ramz Gold", "snippet": "Lattafa Ramz Gold was released in 2021.", "url_raw": "https://one.test/gold"},
        {"title": "Ramz Gold", "snippet": "Ramz Gold was launched in 2022 by Lattafa.", "url_raw": "https://two.test/gold"},
    ]
    evidence = extract_year_evidence(rows, "Lattafa", "Ramz Gold", now_year=2026)
    assert {item.year for item in evidence} == {2021, 2022}
    assert choose_year(evidence) is None

    sauvage = extract_year_evidence(rows, "Dior", "Sauvage", now_year=2026)
    assert sauvage == []


def test_rejects_prefix_flanker_release_statement():
    """A distinct line extension that PRECEDES the requested name (e.g. 'Black
    Oud' or 'Reflection Gold') must not donate its launch year to the base
    fragrance. The existing suffix-flanker guard (Sauvage vs Sauvage Elixir)
    has no mirror-image guard for a modifier word placed in front of the name,
    so 'Amouage Reflection Gold was released in 2007' was previously accepted
    as evidence for plain 'Gold'."""
    rows = [{
        "title": "Amouage Reflection Gold",
        "snippet": "Amouage Reflection Gold was released in 2007 for men.",
        "url_raw": "https://example.test/reflection-gold",
    }]
    evidence = extract_year_evidence(rows, "Amouage", "Gold", now_year=2026)
    assert evidence == []

    oud_rows = [{
        "title": "Lattafa Black Oud",
        "snippet": "Lattafa Black Oud was released in 2018 and quickly became a bestseller.",
        "url_raw": "https://example.test/black-oud",
    }]
    oud_evidence = extract_year_evidence(oud_rows, "Lattafa", "Oud", now_year=2026)
    assert oud_evidence == []

    # The genuine, non-flanker statement (brand directly precedes name) must
    # keep working.
    genuine_rows = [{
        "title": "Amouage Gold",
        "snippet": "Amouage Gold was released in 1983 for women.",
        "url_raw": "https://example.test/gold",
    }]
    genuine_evidence = extract_year_evidence(genuine_rows, "Amouage", "Gold", now_year=2026)
    assert [(item.year, item.domain) for item in genuine_evidence] == [(1983, "example.test")]


def test_decodo_uses_bing_to_corroborate_and_stops_on_google_consensus():
    class Client:
        calls = []

        @classmethod
        def enabled(cls): return True

        @classmethod
        def _post_google(cls, query, timeout):
            cls.calls.append("google")
            return {"organic": [
                {"title": "Ramz Gold", "description": "Lattafa Ramz Gold was released in 2021.", "link": "https://one.test/gold"}
            ]}

        @classmethod
        def _post_bing(cls, query, timeout):
            cls.calls.append("bing")
            return {"organic": [
                {"title": "Ramz Gold", "snippet": "Ramz Gold was launched in 2021 by Lattafa.", "url": "https://two.test/gold"}
            ]}

        @staticmethod
        def _search_entries(payload): return payload["organic"]

        @staticmethod
        def _entry_link(entry): return entry.get("link") or entry.get("url") or ""

        @staticmethod
        def _log_request_failure(kind, exc): raise AssertionError((kind, exc))

    result = resolve_year_decodo("Lattafa", "Ramz Gold", client=Client)
    assert Client.calls == ["google", "bing"]
    assert result["year"] == "2021"
    assert result["year_meta"]["confidence"] == 98
    assert result["year_meta"]["domains"] == ["one.test", "two.test"]


def test_worker_applies_resolved_year_and_keeps_provenance(monkeypatch):
    from scripts import enrichment_worker as worker

    candidate = worker.engine.UnifiedFragrance(name="Ramz Gold", brand="Lattafa", year="")
    resolved = {
        "year": "2021",
        "year_meta": {
            "confidence": 98,
            "source": "decodo_serp_explicit_release",
            "domains": ["one.test", "two.test"],
        },
    }
    monkeypatch.setattr(worker, "resolve_year_decodo", lambda brand, name: resolved)
    result = worker._resolve_missing_year(candidate)
    raw_identity = worker._raw_identity(candidate)
    if result:
        raw_identity["year_meta"] = result["year_meta"]

    assert candidate.year == "2021"
    assert raw_identity["year"] == "2021"
    assert raw_identity["year_meta"]["source"] == "decodo_serp_explicit_release"


def test_worker_year_unknown_does_not_fabricate_but_records_meta(monkeypatch):
    from scripts import enrichment_worker as worker

    candidate = worker.engine.UnifiedFragrance(name="Ramz Gold", brand="Lattafa", year="")
    unresolvable = {
        "year": None,
        "unresolvable": True,
        "year_meta": {
            "confidence": 90,
            "source": "decodo_serp_authoritative_unknown",
            "domains": ["parfumo.com"],
        },
    }
    monkeypatch.setattr(worker, "resolve_year_decodo", lambda brand, name: unresolvable)
    result = worker._resolve_missing_year(candidate)
    assert candidate.year == ""  # never fabricated
    assert result is unresolvable


def test_heal_worthy_drops_year_when_authoritatively_unknown():
    from scripts import enrichment_worker as worker

    payload = {
        "source": "parfumo",
        "quality_status": "partial",
        "raw_identity": {"year_meta": {"source": "decodo_serp_authoritative_unknown"}},
    }
    out = worker._heal_worthy_missing_facts(payload, ["year", "concentration"])
    assert "year" not in out  # genuinely unpublished: stop re-billing Decodo
    assert "concentration" in out  # still resolvable off-page

    # Without the unknown marker, year stays heal-worthy.
    plain = {"source": "parfumo", "quality_status": "partial", "raw_identity": {}}
    assert "year" in worker._heal_worthy_missing_facts(plain, ["year"])


def test_heal_worthy_drops_concentration_when_variant_ambiguous():
    from scripts import enrichment_worker as worker

    payload = {
        "source": "parfumo",
        "quality_status": "partial",
        "raw_identity": {"concentration_meta": {"source": "serp_variant_ambiguous"}},
    }
    out = worker._heal_worthy_missing_facts(payload, ["year", "concentration"])
    assert "concentration" not in out  # name spans concentrations: stop re-billing
    assert "year" in out  # unrelated fact stays heal-worthy

    # Without the ambiguous marker, concentration stays heal-worthy.
    plain = {"source": "parfumo", "quality_status": "partial", "raw_identity": {}}
    assert "concentration" in worker._heal_worthy_missing_facts(plain, ["concentration"])
