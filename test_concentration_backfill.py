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
