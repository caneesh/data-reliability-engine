"""An extension that registers one check and then fails to import (tests/test_extensions.py): the
engine must roll the check back and report the failure."""

from __future__ import annotations

from typing import Any

from hcsc.datalake.dre.checks.base import Check, CheckContext, CheckResult, from_counts
from hcsc.datalake.dre.checks.registry import register_check
from hcsc.datalake.dre.config.models import Dataset


class Half(Check):
    check_id = "X_HALF"

    def required_columns(self, dataset: Dataset, params: Any = None) -> list[str]:
        return []

    def evaluate(self, ctx: CheckContext, event: Any) -> list[CheckResult]:
        return [from_counts(1, 0, observed="1", expected="1")]


register_check(Half())

raise ImportError("a dependency this extension needs is missing")
