"""Static validation of a conf directory: `dre validate` (spec section 3).

Runs the loader's per-file checks, then checks across files: references
exist, key_map covers the key, each feed owner has recipients, and every
dataset's settings resolve. Runtime preconditions (tables and columns exist)
come with the check framework in build step 4.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import ValidationError

from hcsc.datalake.dre.config.loader import (
    Config,
    ConfigError,
    errors_from_validation,
    format_loc,
    load,
)
from hcsc.datalake.dre.config.models import TEMPLATE_PARAMS


def validate_conf(conf_dir: Path | str) -> tuple[Config, list[ConfigError]]:
    config, errors = load(conf_dir)
    errors.extend(cross_check(config))
    return config, sorted(set(errors), key=lambda e: (e.file, e.line, e.field, e.problem))


def cross_check(config: Config) -> list[ConfigError]:
    errors: list[ConfigError] = []

    def err(kind: str, obj_id: str, loc: tuple[Any, ...], problem: str, fix: str) -> None:
        file, line = config.locate(kind, obj_id, loc)
        errors.append(ConfigError(file, line, format_loc(loc), problem, fix))

    datasets = config.datasets
    defaults = config.defaults

    # Feeds: dataset references, one feed per dataset, owner recipients.
    member_of: dict[str, str] = {}
    for feed_id, feed in config.feeds.items():
        for i, ds in enumerate(feed.datasets):
            if ds not in datasets:
                err("feed", feed_id, ("datasets", i), f"unknown dataset {ds!r}",
                    f"add datasets/{ds}.yaml or remove it from this list")
            elif ds in member_of:
                err("feed", feed_id, ("datasets", i), f"dataset {ds!r} is already listed by feed {member_of[ds]!r}",
                    "list each dataset in one feed only")
            else:
                member_of[ds] = feed_id
        if defaults is not None and feed.owner not in defaults.recipients:
            err("feed", feed_id, ("owner",), f"owner {feed.owner!r} has no recipients",
                f"add `{feed.owner}: [address]` under recipients in defaults.yaml")

    # Datasets: upstream, key_map, key-relative fields, settings.
    for ds_id, ds in datasets.items():
        key = set(ds.key)
        for i, up in enumerate(ds.upstream):
            if up == ds_id:
                err("dataset", ds_id, ("upstream", i), "dataset lists itself as upstream", "remove it")
            elif up not in datasets:
                err("dataset", ds_id, ("upstream", i), f"unknown dataset {up!r}",
                    f"add datasets/{up}.yaml or remove it from upstream")
        for up, mapping in ds.key_map.items():
            if up not in ds.upstream:
                err("dataset", ds_id, ("key_map", up), f"key_map names {up!r}, which is not in upstream",
                    f"add {up} to upstream or remove this mapping")
            missing = [c for c in ds.key if c not in mapping]
            extra = [c for c in mapping if c not in key]
            if missing:
                err("dataset", ds_id, ("key_map", up), f"key_map does not cover key column(s) {missing}",
                    "map every key column to its upstream column")
            if extra:
                err("dataset", ds_id, ("key_map", up), f"key_map maps non-key column(s) {extra}",
                    "map only this dataset's key columns")
            if up in datasets:
                not_up_key = [c for c in mapping.values() if c not in datasets[up].key]
                if not_up_key:
                    err("dataset", ds_id, ("key_map", up),
                        f"mapped column(s) {not_up_key} are not in {up}'s key",
                        f"map to columns of {up}'s key {datasets[up].key}")
        for col in ds.key_normalise:
            if col not in key:
                err("dataset", ds_id, ("key_normalise", col), f"{col!r} is not a key column",
                    "normalise key columns only")
        for i, col in enumerate(ds.mismatch_probe_drop):
            if col not in key:
                err("dataset", ds_id, ("mismatch_probe_drop", i), f"{col!r} is not a key column",
                    "list key columns only")
        if ds.owned_columns and not ds.key_map:
            err("dataset", ds_id, ("owned_columns",), "owned_columns needs a key_map",
                "add key_map for the upstream dataset, or remove owned_columns")
        if defaults is not None:
            try:
                config.dataset_settings(ds_id)
            except ValidationError as exc:
                feed = config.feed_of(ds_id)
                where = f"defaults.yaml, feed {feed.feed!r}" if feed else "defaults.yaml"
                for e in exc.errors():
                    name = e["loc"][0]
                    problem = "is not set at any level" if e["type"] == "missing" else e["msg"].lower()
                    err("dataset", ds_id, (name,), f"setting `{name}` {problem}",
                        f"set `{name}` in {where} or this dataset")

    errors.extend(_upstream_cycles(config))

    # Rules: dataset, template params, parent dataset.
    for rule_id, rule in config.rules.items():
        if rule.dataset not in datasets:
            err("rule", rule_id, ("dataset",), f"unknown dataset {rule.dataset!r}",
                f"add datasets/{rule.dataset}.yaml or point the rule at an existing dataset")
        try:
            params = TEMPLATE_PARAMS[rule.template].model_validate(rule.params)
        except ValidationError as exc:
            source = config.sources[("rule", rule_id)]
            errors.extend(errors_from_validation(exc, source.file, source.node, prefix=("params",)))
            continue
        parent = getattr(params, "parent_dataset", None)
        if parent is not None and parent not in datasets:
            err("rule", rule_id, ("params", "parent_dataset"), f"unknown dataset {parent!r}",
                f"add datasets/{parent}.yaml or fix parent_dataset")

    return errors


def _upstream_cycles(config: Config) -> list[ConfigError]:
    """One error per dataset found on an upstream cycle."""
    errors: list[ConfigError] = []
    state: dict[str, int] = {}  # 1 = on the current path, 2 = done
    on_cycle: set[str] = set()

    def visit(ds_id: str, path: list[str]) -> None:
        state[ds_id] = 1
        for up in config.datasets[ds_id].upstream:
            if up not in config.datasets or up == ds_id:
                continue
            if state.get(up) == 1:
                on_cycle.update(path[path.index(up):] + [ds_id])
            elif state.get(up) is None:
                visit(up, path + [up])
        state[ds_id] = 2

    for ds_id in sorted(config.datasets):
        if ds_id not in state:
            visit(ds_id, [ds_id])
    for ds_id in sorted(on_cycle):
        file, line = config.locate("dataset", ds_id, ("upstream",))
        errors.append(ConfigError(file, line, "upstream", f"upstream chain has a cycle through {sorted(on_cycle)}",
                                  "remove one upstream link so the chain ends at a source"))
    return errors
