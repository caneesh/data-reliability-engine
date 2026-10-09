"""T1_ON_TIME: every cadence slot due has a load within sla_hours (spec section 6).

Population: slots whose SLA deadline (slot + sla_hours) falls in the window.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import Check, CheckContext, CheckResult
from hcsc.datalake.dre.checks.tier1.due_loads import judge_due_slots

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset


class OnTime(Check):
    check_id = "T1_ON_TIME"

    def required_columns(self, dataset: Dataset) -> list[str]:
        return [dataset.load_time.column] if dataset.load_time else []

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        return judge_due_slots(
            ctx, event, self.check_id, minimum=1,
            observed="{violations} of {population} due loads had no load within the SLA",
            expected=f"a load within {ctx.settings.sla_hours:g} hours of each cadence slot",
        )
