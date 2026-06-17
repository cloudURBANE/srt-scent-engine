from year_resolver import YearEvidence, choose_year, extract_year_evidence, resolve_year_decodo


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
