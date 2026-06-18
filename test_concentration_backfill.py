"""Tests for the strict, zero-network concentration resolver used by the
engine-gap backfill (scripts/enrich_concentration.py --engine-gap).

These cover the precision contract that makes the backfill safe to point at the
production fragrance_records table: it fills ground-truth concentrations and
never guesses. No database is required.
"""
import importlib.util
from pathlib import Path

import pytest

_MOD_PATH = Path(__file__).resolve().parent / "scripts" / "enrich_concentration.py"
_spec = importlib.util.spec_from_file_location("enrich_concentration_under_test", _MOD_PATH)
ec = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ec)


@pytest.fixture(autouse=True)
def _no_parfinity(monkeypatch):
    """Isolate the name-derivation tier from the Parfinity catalog so name
    cases are deterministic regardless of what the local catalog happens to hold.
    The single Parfinity test re-patches this explicitly."""
    import parfinity

    monkeypatch.setattr(parfinity, "lookup_concentration", lambda brand, name: None)


def _conc(brand, name):
    res = ec.resolve_concentration_strict(brand, name)
    return res["concentration"] if res else None


# --- house-stripping keeps a concentration-bearing house from poisoning detection
def test_strip_house_removes_house_tokens():
    assert ec._strip_house("Oud For Happiness", "Initio Parfums Prives") == "Oud For Happiness"


def test_house_with_parfum_token_does_not_leak():
    # "Initio Parfums Prives" contains "Parfums"; the name has no concentration.
    assert _conc("Initio Parfums Prives", "Oud For Happiness") is None


# --- explicit, distinctive labels are trusted anywhere in the name
@pytest.mark.parametrize(
    "name,expected",
    [
        ("Black Orchid Eau De Parfum", "Eau de Parfum"),
        ("No 5 Eau De Toilette", "Eau de Toilette"),
        ("No 5 Eau De Cologne", "Eau de Cologne"),
        ("Baccarat Rouge 540 Extrait De Parfum", "Extrait"),
        ("Sauvage Elixir", "Elixir"),  # Elixir is a first-class concentration in the scentcast union
        ("Guilty Eau De Parfum Intense Pour Femme", "Eau de Parfum"),
        ("J Adore Body Mist", "Body Spray"),
    ],
)
def test_distinctive_labels_detected(name, expected):
    assert _conc("Some House", name) == expected


# --- the ambiguous bare "Parfum" is only trusted as a trailing modifier
def test_trailing_parfum_is_a_concentration():
    assert _conc("Chanel", "Bleu De Chanel Parfum") == "Parfum"
    assert _conc("Hermes", "Terre d'Hermes Parfum") == "Parfum"


def test_leading_parfum_is_part_of_the_name_not_a_concentration():
    # "Parfum des Merveilles" / "Parfum d'Habit": the word is the proper noun.
    assert _conc("Hermes", "Parfum des Merveilles") is None
    assert _conc("Maitre Parfumeur Et Gantier", "Parfum Dhabit") is None


# --- no concentration signal -> no value (never a guess)
def test_no_signal_returns_none():
    assert _conc("Dior", "Sauvage") is None
    assert _conc("Chanel", "Coco") is None
    assert _conc("Jar", "Bolt Of Lightning") is None


# --- the Parfinity catalog tier is authoritative and consulted first
def test_parfinity_catalog_tier(monkeypatch):
    import parfinity

    monkeypatch.setattr(parfinity, "lookup_concentration", lambda brand, name: "Extrait")
    res = ec.resolve_concentration_strict("Nasomatto", "Black Afgano")
    assert res["concentration"] == "Extrait"
    assert res["concentration_meta"]["source"] == "parfinity_catalog"


def test_name_explicit_provenance():
    res = ec.resolve_concentration_strict("Chanel", "Bleu De Chanel Parfum")
    assert res["concentration_meta"]["source"] == "name_explicit"
    assert res["concentration_meta"]["confidence"] == 100
    assert res["concentration_meta"]["engine_label"] == "Parfum (Profumo)"


# --- Basenotes product-title concentration tier (authoritative, off the canonical
#     "<name> <concentration> by <brand>" page title). Pure helper -- no network.
@pytest.mark.parametrize(
    "title,brand,name,expected",
    [
        # The Lattafa Ramz Gold case: the SERP vote split, but the Basenotes title
        # states it outright.
        ("Ramz Gold Eau de Parfum by Lattafa", "Lattafa", "Ramz Gold", "EDP (Eau de Parfum)"),
        ("Bleu de Chanel Parfum by Chanel", "Chanel", "Bleu de Chanel", "Parfum (Profumo)"),
        ("No 22 Eau de Toilette by Chanel", "Chanel", "No 22", "EDT (Eau de Toilette)"),
        # Size/format noise next to the concentration is tolerated.
        ("Ramz Gold Eau de Parfum 100ml by Lattafa", "Lattafa", "Ramz Gold", "EDP (Eau de Parfum)"),
    ],
)
def test_basenotes_title_extracts_pure_concentration(title, brand, name, expected):
    assert ec._concentration_from_product_title(title, brand, name) == expected


@pytest.mark.parametrize(
    "title,brand,name",
    [
        # Flanker: a distinctive leftover token ("Elixir") means the title is a
        # DIFFERENT product, so the title read must refuse to bind.
        ("Sauvage Elixir Eau de Parfum by Dior", "Dior", "Sauvage"),
        # Wrong product (a name token is absent from the title).
        ("Ramz Silver Eau de Parfum by Lattafa", "Lattafa", "Ramz Gold"),
        # Intense is a flanker-prone label -> excluded from the pure-title set.
        ("Stronger With You Intense by Armani", "Armani", "Stronger With You"),
        # No concentration stated at all -> never a guess.
        ("Sauvage by Dior", "Dior", "Sauvage"),
    ],
)
def test_basenotes_title_rejects_flankers_and_silence(title, brand, name):
    assert ec._concentration_from_product_title(title, brand, name) is None


# --- Online engine-gap resolver: strict -> Decodo Basenotes-title -> Tier-2 SERP.
#     The contract that keeps `--engine-gap --online` safe to point at the
#     production fragrance_records table: it layers only SOURCE-STATED tiers on
#     top of strict and never falls through to a brand/pillar-prior guess.
def _strict_hit(label="EDP (Eau de Parfum)"):
    return {"concentration": label, "concentration_meta": {"source": "name_explicit"}}


def test_online_short_circuits_on_strict(monkeypatch):
    """A strict ground-truth hit returns immediately; no Decodo or SERP call."""
    monkeypatch.setattr(ec, "resolve_concentration_strict", lambda b, n: _strict_hit())
    monkeypatch.setattr(
        ec, "_basenotes_title_concentration",
        lambda b, n: pytest.fail("Decodo tier must not run when strict resolves"),
    )
    monkeypatch.setattr(
        ec, "_engine_tier2", lambda q: pytest.fail("SERP must not run when strict resolves")
    )
    res = ec.resolve_concentration_online("Chanel", "Bleu de Chanel Parfum")
    assert res["concentration_meta"]["source"] == "name_explicit"


def test_online_falls_back_to_basenotes_title(monkeypatch):
    """When strict can't parse, the Decodo Basenotes-title tier fills it."""
    bn = {"concentration": "EDP (Eau de Parfum)", "concentration_meta": {"source": "basenotes_title"}}
    monkeypatch.setattr(ec, "resolve_concentration_strict", lambda b, n: None)
    monkeypatch.setattr(ec, "_basenotes_title_concentration", lambda b, n: bn)
    monkeypatch.setattr(
        ec, "_engine_tier2", lambda q: pytest.fail("SERP must not run when Basenotes resolves")
    )
    res = ec.resolve_concentration_online("Lattafa", "Ramz Gold")
    assert res["concentration_meta"]["source"] == "basenotes_title"


def test_online_falls_through_to_serp_when_browser_enabled(monkeypatch):
    serp = {"concentration": "EDT (Eau de Toilette)", "concentration_meta": {"source": "semantic_engine_v6"}}
    captured = {}
    monkeypatch.setattr(ec, "resolve_concentration_strict", lambda b, n: None)
    monkeypatch.setattr(ec, "_basenotes_title_concentration", lambda b, n: None)
    def _fake_engine_tier2(q):
        captured["q"] = q
        return serp

    monkeypatch.setattr(ec, "_engine_tier2", _fake_engine_tier2)
    res = ec.resolve_concentration_online("Dior", "Sauvage", use_browser=True)
    assert res["concentration_meta"]["source"] == "semantic_engine_v6"
    assert captured["q"] == "Dior Sauvage"


def test_online_tier1_only_never_touches_serp(monkeypatch):
    """use_browser=False (--tier1-only) stops at the offline+Decodo tiers."""
    monkeypatch.setattr(ec, "resolve_concentration_strict", lambda b, n: None)
    monkeypatch.setattr(ec, "_basenotes_title_concentration", lambda b, n: None)
    monkeypatch.setattr(
        ec, "_engine_tier2", lambda q: pytest.fail("SERP must not run under --tier1-only")
    )
    assert ec.resolve_concentration_online("Dior", "Sauvage", use_browser=False) is None


def test_online_surfaces_variant_ambiguous_marker(monkeypatch):
    """When the engine Tier-2 reports variant-ambiguity, the online resolver passes
    the marker through unchanged so run_engine_gap can stamp it (value stays blank)."""
    marker = {
        "concentration": None,
        "variant_ambiguous": True,
        "concentration_meta": {"source": ec.CONCENTRATION_VARIANT_AMBIGUOUS_SOURCE,
                               "attested_concentrations": ["Eau de Parfum", "Eau de Toilette"]},
    }
    monkeypatch.setattr(ec, "resolve_concentration_strict", lambda b, n: None)
    monkeypatch.setattr(ec, "_basenotes_title_concentration", lambda b, n: None)
    monkeypatch.setattr(ec, "_engine_tier2", lambda q: marker)
    res = ec.resolve_concentration_online("Christian Dior", "J Adore", use_browser=True)
    assert res["variant_ambiguous"] is True
    assert res["concentration"] is None


def test_online_never_returns_a_prior_guess(monkeypatch):
    """Ground-truth contract: when every source-stated tier is silent the online
    resolver returns None -- it must NOT fall through to the brand/pillar priors
    that the wardrobe path (resolve_concentration) uses, which would write a guess
    into the engine cache."""
    monkeypatch.setattr(ec, "resolve_concentration_strict", lambda b, n: None)
    monkeypatch.setattr(ec, "_basenotes_title_concentration", lambda b, n: None)
    monkeypatch.setattr(ec, "_engine_tier2", lambda q: None)
    monkeypatch.setattr(
        ec, "resolve_concentration",
        lambda *a, **k: pytest.fail("online must not reach the prior-bearing wardrobe resolver"),
    )
    assert ec.resolve_concentration_online("Creed", "Aventus", use_browser=True) is None


# --- Literal-support gate: the Tier-2 vote can be carried by the brand/pillar
#     prior (an inference), but the engine ground-truth cache must only ever store
#     a concentration a real source row STATED. resolve_concentration_online runs
#     _tier2_serp with require_literal_support=True; the wardrobe path keeps the
#     default (False) and is byte-for-byte unchanged.
from concentration_grabber import ScentProfile  # noqa: E402


def _profile(
    conc, *, conf=80, literal, literal_concs=None,
    second_conc="EDT (Eau de Toilette)", second_conf=0, second_source="none",
):
    """A minimal Tier-2 ScentProfile carrying just the fields the gate reads."""
    return ScentProfile(
        query="q", is_explicit=False,
        primary_concentration=conc, primary_confidence=conf,
        second_concentration=second_conc, second_confidence=second_conf,
        second_source=second_source, flankers_detected=[], scoring_matrix={},
        primary_has_literal_support=literal,
        literal_concentrations=literal_concs if literal_concs is not None else ([conc] if literal else []),
    )


def test_scentprofile_defaults_literal_support_true():
    """Backward-compat: a profile built without the field (old cache payload, or
    the explicit short-circuit) is treated as source-attested so the gate keeps
    accepting it."""
    p = ScentProfile(
        query="q", is_explicit=True, primary_concentration="EDP (Eau de Parfum)",
        primary_confidence=100, second_concentration="EDT (Eau de Toilette)",
        second_confidence=0, second_source="none", flankers_detected=[], scoring_matrix={},
    )
    assert p.primary_has_literal_support is True


def test_tier2_gate_rejects_prior_carried_win_for_engine_cache(monkeypatch):
    """require_literal_support=True drops a confident win that no source row stated."""
    monkeypatch.setattr(
        ec.SemanticScentEngine, "analyze",
        staticmethod(lambda q, *a, **k: _profile("Parfum (Profumo)", literal=False)),
    )
    assert ec._tier2_serp("Roja Dove Scandal", require_literal_support=True) is None


def test_tier2_gate_accepts_source_stated_win_for_engine_cache(monkeypatch):
    """A win a real source row stated clears the gate and is written."""
    monkeypatch.setattr(
        ec.SemanticScentEngine, "analyze",
        staticmethod(lambda q, *a, **k: _profile("EDT (Eau de Toilette)", literal=True)),
    )
    res = ec._tier2_serp("Acqua Di Parma Colonia", require_literal_support=True)
    assert res is not None
    assert res["concentration_meta"]["source"] == "semantic_engine_v6"


def test_tier2_wardrobe_path_keeps_prior_carried_win(monkeypatch):
    """No-regression: the wardrobe resolver leans on priors, so the default
    (require_literal_support=False) still returns a prior-carried win unchanged."""
    monkeypatch.setattr(
        ec.SemanticScentEngine, "analyze",
        staticmethod(lambda q, *a, **k: _profile("Parfum (Profumo)", literal=False)),
    )
    res = ec._tier2_serp("Roja Dove Scandal")  # default: no gate
    assert res is not None
    assert res["concentration_meta"]["source"] == "semantic_engine_v6"


def test_analyze_marks_prior_carried_winner_unsupported(monkeypatch):
    """analyze() itself sets primary_has_literal_support=False when the winning
    concentration came only from `_pillar_default` ballots (source_kind 'pillar')
    -- the brand/pillar prior, not a stated fact."""
    SSE = ec.SemanticScentEngine
    monkeypatch.setattr(ec, "_decodo_concentration_enabled", lambda: False, raising=False)
    monkeypatch.setattr(SSE, "_explicit_intent", staticmethod(lambda q: None))
    monkeypatch.setattr(SSE, "_query_pillar_prior", staticmethod(lambda q: None))
    monkeypatch.setattr(SSE, "_brand_prior", staticmethod(lambda q: ("roja dove", "Parfum (Profumo)")))
    monkeypatch.setattr(SSE, "_retry_fetch", staticmethod(
        lambda page, q, sf, deadline=None: [{"title": "t", "url": "u", "domain": "d"}] if "fragrantica" in sf else []
    ))
    # Every classified row resolves to the prior via the pillar default.
    monkeypatch.setattr(SSE, "_classify_result", staticmethod(
        lambda row, q, brand, brand_prior: ("Parfum (Profumo)", "pillar", 30.0)
    ))
    prof = SSE.analyze("Roja Dove Scandal", use_cache=False, page=object())
    assert prof.primary_concentration == "Parfum (Profumo)"
    assert prof.primary_has_literal_support is False


def test_analyze_marks_source_stated_winner_supported(monkeypatch):
    """analyze() sets primary_has_literal_support=True when the winner came from a
    url/title-modifier ballot (a source row that literally stated it)."""
    SSE = ec.SemanticScentEngine
    monkeypatch.setattr(ec, "_decodo_concentration_enabled", lambda: False, raising=False)
    monkeypatch.setattr(SSE, "_explicit_intent", staticmethod(lambda q: None))
    monkeypatch.setattr(SSE, "_query_pillar_prior", staticmethod(lambda q: None))
    monkeypatch.setattr(SSE, "_brand_prior", staticmethod(lambda q: ("", "")))
    monkeypatch.setattr(SSE, "_retry_fetch", staticmethod(
        lambda page, q, sf, deadline=None: [{"title": "t", "url": "u", "domain": "d"}] if "fragrantica" in sf else []
    ))
    monkeypatch.setattr(SSE, "_classify_result", staticmethod(
        lambda row, q, brand, brand_prior: ("EDT (Eau de Toilette)", "title-modifier", 50.0)
    ))
    prof = SSE.analyze("Acqua Di Parma Colonia", use_cache=False, page=object())
    assert prof.primary_concentration == "EDT (Eau de Toilette)"
    assert prof.primary_has_literal_support is True


def test_analyze_collects_distinct_literal_concentrations(monkeypatch):
    """analyze() records every concentration a real source row stated, so a bare
    name attested as both EDT and EDP carries both -- the variant-ambiguity signal."""
    SSE = ec.SemanticScentEngine
    monkeypatch.setattr(ec, "_decodo_concentration_enabled", lambda: False, raising=False)
    monkeypatch.setattr(SSE, "_explicit_intent", staticmethod(lambda q: None))
    monkeypatch.setattr(SSE, "_query_pillar_prior", staticmethod(lambda q: None))
    monkeypatch.setattr(SSE, "_brand_prior", staticmethod(lambda q: ("", "")))
    rows = [{"title": "a", "url": "u1", "domain": "d"}, {"title": "b", "url": "u2", "domain": "d"}]
    monkeypatch.setattr(SSE, "_retry_fetch", staticmethod(
        lambda page, q, sf, deadline=None: rows if "fragrantica" in sf else []
    ))
    monkeypatch.setattr(SSE, "_classify_result", staticmethod(
        lambda row, q, brand, brand_prior: (
            ("EDT (Eau de Toilette)" if row["title"] == "a" else "EDP (Eau de Parfum)"),
            "title-modifier", 40.0,
        )
    ))
    prof = SSE.analyze("Christian Dior J Adore", use_cache=False, page=object())
    assert set(prof.literal_concentrations) == {"EDP (Eau de Parfum)", "EDT (Eau de Toilette)"}


# --- _engine_tier2: the three engine-cache outcomes (resolve / variant-ambiguous
#     marker / give up). Built on one analyze() pass.
def test_engine_tier2_resolves_confident_source_stated(monkeypatch):
    monkeypatch.setattr(
        ec.SemanticScentEngine, "analyze",
        staticmethod(lambda q, *a, **k: _profile("EDT (Eau de Toilette)", conf=72, literal=True)),
    )
    res = ec._engine_tier2("Acqua Di Parma Colonia")
    assert res["concentration"] == "Eau de Toilette"
    assert res["concentration_meta"]["source"] == "semantic_engine_v6"
    assert not res.get("variant_ambiguous")


def test_engine_tier2_marks_variant_ambiguous_on_genuine_split(monkeypatch):
    """Sub-floor winner AND a second STATED concentration holding real share (>=FLOOR),
    with no dominant leader -> ambiguous marker, concentration left blank."""
    monkeypatch.setattr(
        ec.SemanticScentEngine, "analyze",
        staticmethod(lambda q, *a, **k: _profile(
            "EDT (Eau de Toilette)", conf=40, literal=True,
            literal_concs=["EDP (Eau de Parfum)", "EDT (Eau de Toilette)"],
            second_conc="EDP (Eau de Parfum)", second_conf=33, second_source="consensus",
        )),
    )
    res = ec._engine_tier2("Christian Dior J Adore")
    assert res["variant_ambiguous"] is True
    assert res["concentration"] is None
    assert res["concentration_meta"]["source"] == ec.CONCENTRATION_VARIANT_AMBIGUOUS_SOURCE
    assert res["concentration_meta"]["attested_concentrations"] == ["Eau de Parfum", "Eau de Toilette"]


def test_engine_tier2_resolves_below_floor_dominant_base(monkeypatch):
    """Two concentrations stated but the source-stated leader clearly out-votes the
    lone flanker (>=RATIO, >=DOMINANCE_MIN) -> fill the base, not a 'varies' marker.
    This is the flanker-dilution case (Khamrah = EDP) the count-only gate froze."""
    monkeypatch.setattr(
        ec.SemanticScentEngine, "analyze",
        staticmethod(lambda q, *a, **k: _profile(
            "EDP (Eau de Parfum)", conf=44, literal=True,
            literal_concs=["EDP (Eau de Parfum)", "Parfum (Profumo)"],
            second_conc="Parfum (Profumo)", second_conf=10, second_source="consensus",
        )),
    )
    res = ec._engine_tier2("Lattafa Perfumes Khamrah")
    assert res["concentration"] == "Eau de Parfum"
    assert not res.get("variant_ambiguous")
    assert res["concentration_meta"]["below_floor_dominant"] is True
    assert res["concentration_meta"]["source"] == "semantic_engine_v6"


def test_engine_tier2_lone_flanker_is_not_ambiguous(monkeypatch):
    """A single flanker mention (runner-up below FLOOR) with no dominant leader is
    neither a split nor a confident base -> stays an ordinary gap (no mark, no fill).
    This is the regression the 23/23 misfire exposed."""
    monkeypatch.setattr(
        ec.SemanticScentEngine, "analyze",
        staticmethod(lambda q, *a, **k: _profile(
            "EDP (Eau de Parfum)", conf=30, literal=True,
            literal_concs=["EDP (Eau de Parfum)", "Parfum (Profumo)"],
            second_conc="Parfum (Profumo)", second_conf=12, second_source="consensus",
        )),
    )
    assert ec._engine_tier2("Some Niche Flanker") is None


def test_engine_tier2_gives_up_on_weak_single_signal(monkeypatch):
    """Sub-floor with only one concentration stated is NOT ambiguous -- it stays an
    ordinary gap (a future page-scrape could still resolve it). No marker."""
    monkeypatch.setattr(
        ec.SemanticScentEngine, "analyze",
        staticmethod(lambda q, *a, **k: _profile(
            "EDP (Eau de Parfum)", conf=47, literal=False,
            literal_concs=["EDP (Eau de Parfum)"],
        )),
    )
    assert ec._engine_tier2("Versace Rose Flamboyante") is None


def test_engine_tier2_rejects_confident_prior_carried_win(monkeypatch):
    """Confident but no source row stated the winner, and <2 stated -> None (the
    ground-truth gate still holds; nothing is marked ambiguous)."""
    monkeypatch.setattr(
        ec.SemanticScentEngine, "analyze",
        staticmethod(lambda q, *a, **k: _profile("Parfum (Profumo)", conf=80, literal=False, literal_concs=[])),
    )
    assert ec._engine_tier2("Roja Dove Scandal") is None


def test_concentration_meta_marks_ambiguous_helper():
    from enrichment_facts import (
        CONCENTRATION_VARIANT_AMBIGUOUS_SOURCE,
        concentration_meta_marks_ambiguous,
    )
    assert concentration_meta_marks_ambiguous(
        {"source": CONCENTRATION_VARIANT_AMBIGUOUS_SOURCE}
    )
    assert not concentration_meta_marks_ambiguous({"source": "semantic_engine_v6"})
    assert not concentration_meta_marks_ambiguous(None)
    assert not concentration_meta_marks_ambiguous("nope")
