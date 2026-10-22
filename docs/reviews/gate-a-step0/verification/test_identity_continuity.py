"""Gate A step 0, U-4: when is a legacy (encoding 1) identity PROVABLY the same record as an encoding 2
successor? Verification code only (pure Python, no engine import).

Encoding 1 writes parts joined with '|' and NULL as '<null>'. A legacy key_value is provably one tuple
only when it decodes to exactly one candidate tuple by syntax alone:
  - a part whose normalised type is not a string type (DATE, TIMESTAMP, integral, DECIMAL, BOOLEAN)
    can hold neither '|' nor the text '<null>', so its token is unambiguous;
  - a string part can hold both, so a split point or a '<null>' token in it may be ambiguous.
Data present upstream today never proves continuity: a colliding record may have existed when the key
was flagged and left since.
"""

from __future__ import annotations

import re
from itertools import combinations

STRING = "string"
TYPED = re.compile(r"\d{4}-\d{2}-\d{2}")  # a typed part: its canonical rendering (here a DATE) is fixed


def candidates(key_value: str, kinds: list[str]) -> set[tuple[str | None, ...]]:
    """Every tuple whose encoding-1 string is key_value, given each part's kind."""
    cuts = [i for i, ch in enumerate(key_value) if ch == "|"]
    found: set[tuple[str | None, ...]] = set()
    for chosen in combinations(cuts, len(kinds) - 1):
        bounds = [-1, *chosen, len(key_value)]
        tokens = [key_value[bounds[i] + 1:bounds[i + 1]] for i in range(len(kinds))]
        if any(k is not STRING and t != "<null>" and not k.fullmatch(t) for k, t in zip(kinds, tokens)):
            continue  # a typed token must be its type's rendering (which never holds '|' or '<null>')
        options: list[list[str | None]] = []
        for k, t in zip(kinds, tokens):
            if t == "<null>":
                options.append([None, "<null>"] if k is STRING else [None])
            else:
                options.append([t])
        partial: list[tuple[str | None, ...]] = [()]
        for opts in options:
            partial = [p + (o,) for p in partial for o in opts]
        found.update(partial)
    return found


def provable(key_value: str, kinds: list[str]) -> bool:
    return len(candidates(key_value, kinds)) == 1


KINDS = [STRING, STRING, TYPED, STRING]  # sub_id, mem_nbr, effective date (parsed), agreement id


def test_a_plain_key_is_provable() -> None:
    assert provable("123401|01|2026-01-01|AGR-A", KINDS)


def test_a_null_token_in_a_string_part_is_not_provable() -> None:
    """NULL and the literal text '<null>' encode alike in a string part."""
    assert candidates("<null>|01|2026-01-01|AGR-A", KINDS) == {(None, "01", "2026-01-01", "AGR-A"),
                                                                ("<null>", "01", "2026-01-01", "AGR-A")}
    assert not provable("<null>|01|2026-01-01|AGR-A", KINDS)


def test_a_null_token_in_a_typed_part_is_provable() -> None:
    assert provable("123401|01|<null>|AGR-A", KINDS)


def test_an_extra_separator_in_string_parts_is_not_provable() -> None:
    assert candidates("12|34|01|2026-01-01|AGR-A", KINDS) == {("12|34", "01", "2026-01-01", "AGR-A"),
                                                              ("12", "34|01", "2026-01-01", "AGR-A")}


def test_a_typed_part_pins_the_split() -> None:
    """A separator can only fall inside a string part; here the date pins every cut."""
    assert candidates("1|2026-01-01|x|y", [STRING, TYPED, STRING]) == {("1", "2026-01-01", "x|y")}


def test_the_examination_collisions_are_not_provable() -> None:
    """a19 / a20 shapes: split pairs and null against '<null>'."""
    assert not provable("A|B|C", [STRING, STRING])
    assert not provable("<null>|C", [STRING, STRING])
