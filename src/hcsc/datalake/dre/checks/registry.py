"""The check registry (spec section 2, "Extension points").

Checks register themselves (`register_check`) in the module that defines them; the built-in
modules under checks/tier1 and checks/hop are imported on first use (`Registry._discover`).
Which checks run for a dataset, and with what parameters, is in checks/patterns.py.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from hcsc.datalake.dre.registry import Registry

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.base import Check

CHECKS: Registry[Check] = Registry("check", ("hcsc.datalake.dre.checks.tier1", "hcsc.datalake.dre.checks.hop"))


def register_check(check: Check) -> Check:
    """Called by a check's module; the check's `check_id` must be unique."""
    return CHECKS.register(check.check_id, check)
