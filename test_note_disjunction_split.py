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
from fragrance_parser_full_rewrite_fixed import (
    NotesList,
    TextSanitizer,
    _collapse_synonym_paren,
    _split_note_disjunction,
    dedupe_notes,
    normalize_notes,
    sanitize_note_tokens,
)


def test_split_or_and_slash_disjunctions():
    # The exact screenshot base slot: "Pepperwood or Hercules Club" splits, and
    # the redundant "Agarwood (Oud)" restatement folds to the canonical "Oud".
    assert dedupe_notes(
        ["Agarwood (Oud)", "Ambergris", "Pepperwood or Hercules Club"]
    ) == [["Oud", "Ambergris", "Pepperwood", "Hercules Club"]]

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


def test_redundant_synonym_parenthetical_collapses():
    # Outer name and parenthetical are the same note under two synonyms — fold
    # to the single canonical spelling.
    assert dedupe_notes(["Agarwood (Oud)"]) == [["Oud"]]
    assert dedupe_notes(["Olibanum (Frankincense)"]) == [["Frankincense"]]
    assert _collapse_synonym_paren("Oud (Agarwood)") == "Oud"


def test_clarifying_parentheticals_preserved():
    # Two genuinely distinct notes, or provenance — the parenthetical must stay.
    for token in (
        "Cistus (Labdanum)",
        "Neroli (Orange Blossom)",
        "Bergamot Essence (Calabrian)",
        "Iris (Morocco)",
        "Vetiver (Bourbon, Java, Haiti)",
    ):
        assert _collapse_synonym_paren(token) == token, token


def test_invisible_glyphs_stripped():
    # Variation selector (U+FE0F) trailing a trademark glyph must not survive.
    assert "️" not in TextSanitizer.clean("Ambrox®️Super")
    assert dedupe_notes(["Ambrox®️Super"]) == [["AmbroxSuper"]]
    # Plain trademark stripping still works.
    assert dedupe_notes(["Pepperwood™"]) == [["Pepperwood"]]


def test_serve_backstop_cleans_unnormalized_cache_notes():
    # Simulates a detail hydrated straight from a cache row (no normalize_notes):
    # every dirty-token class is repaired in one pass.
    nl = NotesList(
        has_pyramid=True,
        top=["Pink Pepper"],
        heart=["Sea Notes", "Sea Salt"],
        base=["Agarwood (Oud)", "Ambergris", "Pepperwood or Hercules Club", "Akigalawood™"],
    )
    assert sanitize_note_tokens(nl) is True
    assert nl.base == ["Oud", "Ambergris", "Pepperwood", "Hercules Club", "Akigalawood"]
    # Untouched layers stay exactly as-is.
    assert nl.top == ["Pink Pepper"]
    assert nl.heart == ["Sea Notes", "Sea Salt"]


def test_backstop_is_noop_after_normalize():
    # The live path already runs normalize_notes; the backstop must not re-edit
    # its output (idempotent) and must not introduce broad cross-layer folding.
    nl = NotesList(
        has_pyramid=True,
        top=["Pink Pepper"],
        heart=["Sea Notes", "Sea Salt"],
        base=["Agarwood (Oud)", "Pepperwood or Hercules Club"],
    )
    normalize_notes(nl)
    snapshot = (list(nl.top), list(nl.heart), list(nl.base), list(nl.flat))
    assert sanitize_note_tokens(nl) is False
    assert (nl.top, nl.heart, nl.base, nl.flat) == snapshot


def test_backstop_does_not_fold_across_layers():
    # A note shared across layers, or two distinct qualified variants, must be
    # left in place — the backstop only repairs tokens, it does not dedupe like
    # normalize_notes would.
    nl = NotesList(
        has_pyramid=True,
        top=["Patchouli"],
        heart=["Russian Rose", "Turkish Rose"],
        base=["Patchouli", "Vanilla"],
    )
    sanitize_note_tokens(nl)
    assert nl.top == ["Patchouli"]
    assert nl.heart == ["Russian Rose", "Turkish Rose"]
    assert nl.base == ["Patchouli", "Vanilla"]  # not dropped despite top Patchouli


if __name__ == "__main__":
    test_split_or_and_slash_disjunctions()
    test_compounds_and_plain_notes_unchanged()
    test_split_never_emits_empty_or_singleton_fragments()
    test_redundant_synonym_parenthetical_collapses()
    test_clarifying_parentheticals_preserved()
    test_invisible_glyphs_stripped()
    test_serve_backstop_cleans_unnormalized_cache_notes()
    test_backstop_is_noop_after_normalize()
    test_backstop_does_not_fold_across_layers()
    print("test_note_disjunction_split: PASS")
