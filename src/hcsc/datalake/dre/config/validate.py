"""Static validation of a conf directory: `dre validate` (spec section 3).

Runs the loader's per-file checks, then checks across files: references
exist, key_map covers the key, owners have recipients, time columns needed by
hop checks are set, and every dataset's settings resolve. Errors fail
validation; warnings are reported only. SQL fragments are not parsed here:
dry-run does that against Spark (build step 4). Runtime preconditions (tables
and columns exist) also come in step 4.
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

# Checks whose evaluation window is defined by load time (spec section 6).
LOAD_TIME_CHECKS = ("T1_ON_TIME", "T1_ZERO_ROWS", "T1_VOLUME", "T1_KEY_NULLS")
# Cause checks that compare load times (spec section 7, key missing or stale).
LOAD_TIME_CAUSES = ("NOT_RUN", "OLDER_VERSION_WRITTEN_LATER")

_ENGINE_ROOT = Path(__file__).resolve()


def validate_conf(conf_dir: Path | str) -> tuple[Config, list[ConfigError], list[ConfigError]]:
    """(config, errors, warnings) for a conf directory."""
    conf_dir = Path(conf_dir)
    config, issues = load(conf_dir)
    feeds_dir = str(conf_dir / "feeds")
    feeds_complete = not any(e.file.startswith(feeds_dir) for e in issues)
    issues.extend(cross_check(config, feeds_complete=feeds_complete))
    ordered = sorted(set(issues), key=lambda e: (e.file, e.line, e.field, e.problem))
    return (
        config,
        [e for e in ordered if e.level == "error"],
        [e for e in ordered if e.level == "warning"],
    )


def cross_check(config: Config, feeds_complete: bool = True) -> list[ConfigError]:
    """Checks across files. feeds_complete=False skips checks that need every feed loaded."""
    errors: list[ConfigError] = []

    def err(kind: str, obj_id: str, loc: tuple[Any, ...], problem: str, fix: str, level: str = "error") -> None:
        file, line = config.locate(kind, obj_id, loc)
        errors.append(ConfigError(file, line, format_loc(loc), problem, fix, level))

    datasets = config.datasets
    defaults = config.defaults
    recipients = defaults.recipients if defaults is not None else None

    def check_owner(kind: str, obj_id: str, owner: str) -> None:
        if recipients is not None and owner not in recipients:
            err(kind, obj_id, ("owner",), f"owner {owner!r} has no recipients",
                f"add `{owner}: [address]` under recipients in defaults.yaml")

    if defaults is not None and defaults.hmac_secret_file is not None:
        root = _containing_root(Path(defaults.hmac_secret_file), config.conf_dir)
        if root is not None:
            err("defaults", "defaults", ("hmac_secret_file",), f"hmac_secret_file is inside {root}",
                "keep the secret in a protected file outside the repository and the conf directory")

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
        check_owner("feed", feed_id, feed.owner)

    # Datasets.
    for ds_id, ds in datasets.items():
        key = set(ds.key)

        # Feed datasets take their owner from the feed; table-wide datasets need their own.
        if ds_id in member_of:
            if ds.owner is not None:
                err("dataset", ds_id, ("owner",), f"owner is set, but feed {member_of[ds_id]!r} owns this dataset",
                    "remove owner; the feed's owner receives its results")
            if ds.expectation_version is not None:
                err("dataset", ds_id, ("expectation_version",),
                    f"expectation_version is set, but feed {member_of[ds_id]!r} versions this dataset's checks",
                    "remove expectation_version; bump the feed's instead")
        elif feeds_complete:
            if ds.owner is None:
                err("dataset", ds_id, ("owner",), "a dataset that no feed lists needs an owner",
                    "add owner: (a table-wide dataset), or list the dataset in its feed")
            else:
                check_owner("dataset", ds_id, ds.owner)
            if ds.expectation_version is None:
                err("dataset", ds_id, ("expectation_version",), "a dataset that no feed lists needs an expectation_version",
                    "add expectation_version: 1, and bump it when this dataset's checks change")
            if ds.feed_filter is not None:
                err("dataset", ds_id, ("feed_filter",), "a dataset that no feed lists cannot have a feed_filter",
                    "remove feed_filter (table-wide datasets cover the whole table), or list the dataset in its feed")

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
                if datasets[up].record_time is None:
                    err("dataset", up, ("record_time",),
                        f"record_time is not set, and {ds_id!r} compares against this dataset through key_map",
                        "add record_time: {column: ..., format: ...}")

        if ds.key_map and ds.record_time is None:
            err("dataset", ds_id, ("record_time",), "a dataset with key_map needs record_time",
                "add record_time: {column: ..., format: ...}")

        if ds.load_time is None:
            causes = f"; cause checks {', '.join(LOAD_TIME_CAUSES)} cannot be confirmed" if ds.key_map else ""
            err("dataset", ds_id, ("load_time",),
                f"load_time is not set: {', '.join(LOAD_TIME_CHECKS)} will be DID_NOT_RUN{causes}",
                "add load_time: {column, format, granularity} once the column is confirmed",
                level="warning")

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

    # Rules: dataset, owner, template params, parent dataset.
    for rule_id, rule in config.rules.items():
        if rule.dataset not in datasets:
            err("rule", rule_id, ("dataset",), f"unknown dataset {rule.dataset!r}",
                f"add datasets/{rule.dataset}.yaml or point the rule at an existing dataset")
        check_owner("rule", rule_id, rule.owner)
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


def _git_root(start: Path) -> Path | None:
    for parent in (start, *start.parents):
        if (parent / ".git").exists():
            return parent
    return None


def _containing_root(path: Path, conf_dir: Path) -> Path | None:
    """The conf directory or repository that contains path, if any."""
    target = path.resolve()
    conf = conf_dir.resolve()
    roots = [conf, _git_root(conf), _git_root(_ENGINE_ROOT.parent)]
    for root in roots:
        if root is not None and (target == root or root in target.parents):
            return root
    return None


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
