"""The registry of generic cause probes (spec section 7).

A probe type is a parameter model and a function, defined together in causes/probes.py with
the `probe` decorator. A feed configures probes under `probes:`, a map from a config key to
the probe's parameters (or a list of parameter sets, any of which may confirm the cause). The
pattern YAML says which cause code uses which probe type and which config key. A probe whose
parameters are missing or null is NOT_READY, never RULED_OUT. `builtin` names engine logic
registered by cause code (causes/base.py) and takes no parameters.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

from hcsc.datalake.dre.config.models import Model
from hcsc.datalake.dre.registry import Registry

if TYPE_CHECKING:
    from hcsc.datalake.dre.causes.base import CauseContext, Failure, Outcome

BUILTIN = "builtin"


@dataclass(frozen=True)
class Probe:
    name: str
    params_model: type[Model]  # every field None by default; the probe runs only when all are set (is_ready)
    run: Callable[[CauseContext, Any, Failure], Outcome]


PROBES: Registry[Probe] = Registry("probe", ("hcsc.datalake.dre.causes",))


def probe(name: str, params_model: type[Model]) -> Callable[[Callable[..., Outcome]], Callable[..., Outcome]]:
    """Register the decorated function as the probe type `name`, taking `params_model`."""
    def register(fn: Callable[..., Outcome]) -> Callable[..., Outcome]:
        PROBES.register(name, Probe(name, params_model, fn))
        return fn
    return register


def is_ready(params: Model | None) -> bool:
    """A probe can run only when every parameter it declares is set."""
    if params is None:
        return False
    return all(getattr(params, name) is not None for name in type(params).model_fields
               if name not in ("id", "code_ref"))


def parameter_sets(raw: Any) -> list[Any]:
    """A probe key's value as a list of parameter sets (a single mapping, a list, or null)."""
    if raw is None:
        return []
    return list(raw) if isinstance(raw, list) else [raw]
