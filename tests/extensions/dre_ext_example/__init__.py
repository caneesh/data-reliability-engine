"""An example extension package (spec section 2, "Extension packages"): one capability of each
kind, registered the way the engine's own modules register theirs. tests/test_extensions.py
installs it through an entry point in the group dre.extensions and loads it by name."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from hcsc.datalake.dre.causes.base import CauseContext, Failure, Outcome, builtin, confirmed, ruled_out
from hcsc.datalake.dre.checks.base import Check, CheckContext, CheckResult, from_counts
from hcsc.datalake.dre.checks.calendars import CALENDARS, Calendar
from hcsc.datalake.dre.checks.keys import Normaliser, register_normaliser
from hcsc.datalake.dre.checks.registry import register_check
from hcsc.datalake.dre.checks.rules.template import TemplateSpec, register_template
from hcsc.datalake.dre.config.models import Dataset, Model
from hcsc.datalake.dre.config.probes import probe
from hcsc.datalake.dre.notify.sinks import Message, SinkKind, SINKS


class RowsAtMostParams(Model):
    max_rows: int = 2


class RowsAtMost(Check):
    """FAILED when the table holds more than `max_rows` rows."""

    check_id = "X_ROWS_AT_MOST"
    params_model = RowsAtMostParams

    def required_columns(self, dataset: Dataset, params: Any = None) -> list[str]:
        return []

    def evaluate(self, ctx: CheckContext, event: Any) -> list[CheckResult]:
        limit = ctx.params.max_rows if ctx.params else 2
        [row] = ctx.spark.sql(f"SELECT COUNT(*) AS n FROM {ctx.dataset.table}").collect()
        return [from_counts(row.n, max(row.n - limit, 0), observed=f"{row.n} rows", expected=f"at most {limit} rows")]


register_check(RowsAtMost())


class Flag(Model):
    value: str | None = None


@probe("x_flag", Flag)
def flagged(cx: CauseContext, params: Flag, failure: Failure) -> Outcome:
    return confirmed(value=params.value) if params.value == "yes" else ruled_out(value=params.value)


@builtin("X_ALWAYS")
def always(cx: CauseContext, failure: Failure) -> Outcome:
    return confirmed(ref=failure.ref)


class AtMostRows(Model):
    max: int


register_template(TemplateSpec(
    name="x_at_most_rows", params_model=AtMostRows,
    sql=lambda table, feed_filter, group_by, max: (
        f"SELECT {''.join(f'{g} AS g_{i}, ' for i, g in enumerate(group_by, 1))}COUNT(*) AS population, "
        f"CASE WHEN COUNT(*) > {max} THEN COUNT(*) - {max} ELSE 0 END AS violations FROM {table}"
        f"{' WHERE ' + feed_filter if feed_filter else ''}{' GROUP BY ' + ', '.join(group_by) if group_by else ''}"),
    columns=lambda ds, p: [], arguments=lambda ds, p, related: {"max": p.max},
    what=lambda p: f"rows over the {p.max} allowed", unit="rows",
))

register_normaliser(Normaliser("x_reverse", lambda c, a: f"reverse({c})", "x_reverse"))

CALENDARS.register("X_ODD_DAYS", Calendar("X_ODD_DAYS", lambda day: day.day % 2 == 1, "extension"))


class PigeonConfig(Model):
    kind: str
    loft: str


@dataclass
class Pigeon:
    loft: str
    kind: str = "x_pigeon"

    def send(self, messages: list[Message]) -> list[str]:
        return [f"x_pigeon for {m.owner}: {m.subject} (flown to {self.loft})" for m in messages]


SINKS.register("x_pigeon", SinkKind("x_pigeon", PigeonConfig, lambda c: Pigeon(c.loft)))
