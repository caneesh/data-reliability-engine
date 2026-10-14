"""Static checks on SQL that comes from configuration (spec section 9, rule 10).

Configuration SQL (feed_filter, hop_exclusions, rule params, derived expressions, probe
conditions, and the rule templates under conf/templates) runs inside a query the engine
builds, so a command cannot parse. Two things a query can still do are refused here: Spark's
`TRANSFORM ... USING` clause, which runs an external program, and `java_method` / `reflect`,
which call Java methods. The write and DDL verbs are refused too, as defence in depth. String
literals and comments are blanked before matching, so `status = 'LOAD'` or a `-- delete` note
is fine; only the words as SQL count.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

_WORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|TRUNCATE|CREATE|LOAD|GRANT|REVOKE|MSCK|REFRESH|CACHE|UNCACHE|"
    r"ANALYZE|JAVA_METHOD|REFLECT\w*|ADD\s+(?:JAR|FILE|ARCHIVE))\b", re.I)
# The script transform (TRANSFORM (...) USING 'cmd', MAP ... USING 'cmd', REDUCE ... USING 'cmd') runs an
# external program. Its USING takes a string (blanked below), a join's USING takes a column list in
# parentheses, and every other USING belongs to a statement refused above: a USING not followed by
# "(" is the script form. The word TRANSFORM alone is Spark's higher-order function and is allowed.
_SCRIPT_USING = re.compile(r"\bUSING\b(?!\s*\()", re.I)
# A single-quoted literal (with '' and \' escapes), a double-quoted one, a -- comment, a /* */ comment.
_BLANK = re.compile(r"'(?:[^'\\]|\\.|'')*'|\"(?:[^\"\\]|\\.)*\"|--[^\n]*|/\*.*?\*/", re.S)


@dataclass(frozen=True)
class SqlProblem:
    line: int  # 1-based, within the text
    problem: str
    fix: str


def blank_literals(text: str) -> str:
    """The text with literals and comments replaced by spaces of the same length (newlines kept),
    so positions and line numbers still match the original."""
    return _BLANK.sub(lambda m: re.sub(r"[^\n]", " ", m.group(0)), text)


def forbidden_sql(text: str) -> SqlProblem | None:
    """The first forbidden word used as SQL, with its line; None when the text is clean."""
    blanked = blank_literals(text)
    hit = _WORDS.search(blanked)
    script = _SCRIPT_USING.search(blanked)
    if hit is None or (script is not None and script.start() < hit.start()):
        hit = script
    if hit is None:
        return None
    word = re.sub(r"\s+", " ", hit.group(0).upper())
    if hit is script:
        word = "a script transform (TRANSFORM, MAP or REDUCE ... USING)"
    return SqlProblem(text.count("\n", 0, hit.start()) + 1,
                      f"{word} is not allowed in configuration SQL (hard rule 1: read-only)",
                      "configuration SQL is a query over the tables it names: no commands, no TRANSFORM, "
                      "no java_method or reflect")
