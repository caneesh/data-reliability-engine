"""Extension packages (spec section 2, "Extension packages").

A separately installed package can add checks, probes, builtin causes, rule templates, key
normalisers, calendars and notification sinks: it registers them the way the engine's own
modules do, and declares one entry point in the group `dre.extensions` whose value is the
module to import. Nothing loads unless `defaults.yaml` lists the entry point's name under
`extensions:`, so configuration says what runs, but never names code paths: the entry point is
part of an installed release, checked by `dre guard` in that package's CI.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from importlib.metadata import EntryPoint, entry_points
from typing import Any

GROUP = "dre.extensions"


@dataclass(frozen=True)
class Loaded:
    name: str
    module: str
    registered: dict[str, list[str]] = field(default_factory=dict)  # registry kind -> ids this extension added

    def summary(self) -> str:
        parts = [f"{kind} {', '.join(ids)}" for kind, ids in self.registered.items() if ids]
        return f"{self.name} ({self.module}): " + ("; ".join(parts) if parts else "registered nothing")


_LOADED: dict[str, Loaded] = {}


def available() -> dict[str, EntryPoint]:
    """Installed extensions by entry point name."""
    return {ep.name: ep for ep in entry_points(group=GROUP)}


def _registries() -> dict[str, Any]:
    from hcsc.datalake.dre.causes.base import BUILTINS
    from hcsc.datalake.dre.checks.calendars import CALENDARS
    from hcsc.datalake.dre.checks.keys import NORMALISERS
    from hcsc.datalake.dre.checks.registry import CHECKS
    from hcsc.datalake.dre.checks.rules.template import TEMPLATES
    from hcsc.datalake.dre.config.probes import PROBES
    from hcsc.datalake.dre.notify.sinks import SINKS

    return {"checks": CHECKS, "probes": PROBES, "builtin causes": BUILTINS, "rule templates": TEMPLATES,
            "normalisers": NORMALISERS, "calendars": CALENDARS, "sinks": SINKS}


def load_extensions(names: list[str]) -> tuple[list[Loaded], list[str]]:
    """Import the named extensions once each: (what loaded, problems). A name with no entry point,
    an import error, or a registration that collides with an existing id is a problem, reported
    rather than raised; the engine runs with what did load."""
    loaded: list[Loaded] = []
    problems: list[str] = []
    installed = available() if names else {}
    registries = _registries() if names else {}
    for name in names:
        if name in _LOADED:
            loaded.append(_LOADED[name])
            continue
        ep = installed.get(name)
        if ep is None:
            problems.append(f"extension {name!r} is not installed: no entry point {name!r} in group {GROUP} "
                            f"(installed: {sorted(installed) or 'none'})")
            continue
        before = {kind: set(reg.ids()) for kind, reg in registries.items()}
        try:
            ep.load()
        except Exception as exc:  # an import error, or a duplicate id from Registry.register
            problems.append(f"extension {name!r} ({ep.value}) failed to load: {type(exc).__name__}: {exc}")
            _roll_back(registries, before, ep.value)  # whatever it registered before failing goes too
            continue
        added = {kind: sorted(set(reg.ids()) - before[kind]) for kind, reg in registries.items()}
        record = Loaded(name, ep.value, {k: v for k, v in added.items() if v})
        _LOADED[name] = record
        loaded.append(record)
    return loaded, problems


def _roll_back(registries: dict[str, Any], before: dict[str, set[str]], module: str) -> None:
    """Unregister what a failed load added, and forget the module so a later attempt imports again."""
    import sys

    for kind, reg in registries.items():
        for item_id in set(reg.ids()) - before[kind]:
            reg.unregister(item_id)
    for name in [m for m in sys.modules if m == module or m.startswith(module + ".")]:
        del sys.modules[name]


def sync_extensions(names: list[str]) -> tuple[list[Loaded], list[str]]:
    """The conf's list is the whole truth: unload any extension not in it (a process that loads
    one conf after another), then load the listed ones."""
    for name in [n for n in _LOADED if n not in names]:
        unload(name)
    return load_extensions(names)


def loaded() -> list[Loaded]:
    return list(_LOADED.values())


def unload(name: str) -> None:
    """Forget an extension and unregister what it added (tests; a process never unloads otherwise)."""
    import sys

    record = _LOADED.pop(name, None)
    if record is None:
        return
    for kind, ids in record.registered.items():
        for item_id in ids:
            _registries()[kind].unregister(item_id)
    for module in [m for m in sys.modules if m == record.module or m.startswith(record.module + ".")]:
        del sys.modules[module]  # so a later load imports, and registers, again
