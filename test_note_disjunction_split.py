"""Note disjunction-split cleaning (dedupe_notes).

Some sources record one note slot under two interchangeable common names joined
by a disjunction — e.g. Tom Ford Oud Minerale's base "Pepperwood or Hercules
Club", or "Mastic or Lentisque" / "Benzoin / Tolu Balsam" seen in the live
caches. The note tokenizer splits on commas/semicolons/pipes but historically
not on these, so the whole phrase leaked through as one dirty "note".

These checks pin two things:
  1. the disjunctive " or " and "/" ARE split into clean individual notes;
  2. " and " / "&" and ordinary tokens are LEFT INTACT (regression guard) —
     those routinely form genuine single compound note names.

Script-style (has __main__) so run_tests.py runs it as a plain script.
"""
from fragrance_parser_full_rewrite_fixed import _split_note_disjunction, dedupe_notes


def test_split_or_and_slash_disjunctions():
    # The exact screenshot case: a single base slot with two common names.
    assert dedupe_notes(
        ["Agarwood (Oud)", "Ambergris", "Pepperwood or Hercules Club"]
    ) == [["Agarwood (Oud)", "Ambergris", "Pepperwood", "Hercules Club"]]

    # Verbatim dirty tokens observed in fg_cache.
    assert dedupe_notes(["Mastic or Lentisque"]) == [["Mastic", "Lentisque"]]
    assert dedupe_notes(["Benzoin / Tolu Balsam"]) == [["Benzoin", "Tolu Balsam"]]

    # Helper level, case-insensitive on the connector.
    assert _split_note_disjunction("Pepperwood OR Hercules Club") == [
        "Pepperwood",
        "Hercules Club",
    ]


def test_compounds_and_plain_notes_unchanged():
    # " and " / "&" are genuine compound names — never split.
    for token in (
        "Pink & Black Pepper",
        "Pink and Black Pepper",
        "Saffron & Leather Accord",
        "Turkish Rose Essence and Absolute",
        "Water & Salt",
    ):
        assert _split_note_disjunction(token) == [token], token

    # Words that merely *contain* the letters "or" are untouched (no whitespace
    # boundary), so identity folding still works downstream.
    for token in ("Orange", "Coriander", "Orris", "Cardamom", "Forest Air"):
        assert _split_note_disjunction(token) == [token], token

    # Ordinary clean layers pass straight through, synonyms still fold.
    assert dedupe_notes(["Bergamot", "Vanilla", "Sandalwood"]) == [
        ["Bergamot", "Vanilla", "Sandalwood"]
    ]
    assert dedupe_notes(["Cedarwood", "Cedar"]) == [["Cedar"]]


def test_split_never_emits_empty_or_singleton_fragments():
    # Degenerate inputs must not yield empty/garbage tokens.
    assert _split_note_disjunction("Vanilla") == ["Vanilla"]
    assert _split_note_disjunction("a or b") == ["a or b"]  # fragments too short
    assert _split_note_disjunction("Rose or ") == ["Rose or "]  # one real side only


if __name__ == "__main__":
    test_split_or_and_slash_disjunctions()
    test_compounds_and_plain_notes_unchanged()
    test_split_never_emits_empty_or_singleton_fragments()
    print("test_note_disjunction_split: PASS")
