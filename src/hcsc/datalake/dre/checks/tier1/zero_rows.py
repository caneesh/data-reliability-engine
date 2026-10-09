"""T1_ZERO_ROWS: each load due wrote at least min_rows_per_load rows (spec section 6).

Population: slots whose SLA deadline (slot + sla_hours) falls in the window, the same
slots T1_ON_TIME judges; rows loaded in [slot, slot + SLA) count for the slot. A load due
that wrote fewer rows: FAILED (also on an empty table). No load due: DID_NOT_RUN /
empty_population.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import Check, CheckContext, CheckResult
from hcsc.datalake.dre.checks.tier1.due_loads import judge_due_slots

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset


class ZeroRows(Check):
    check_id = "T1_ZERO_ROWS"

    def required_columns(self, dataset: Dataset) -> list[str]:
        return [dataset.load_time.column] if dataset.load_time else []

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        minimum = ctx.settings.min_rows_per_load
        return judge_due_slots(
            ctx, event, self.check_id, minimum=minimum,
            observed="{violations} of {population} due loads wrote fewer rows than the minimum",
            expected=f"at least {minimum} rows per load",
        )
