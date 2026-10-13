"""Static validation of a conf directory: `dre validate` (spec section 3).

Runs the loader's per-file checks, then checks across files: references
exist, key_map covers the key, owners have recipients, time columns needed by
hop checks are set, and every dataset's settings resolve. Errors fail
validation; warnings are reported only. SQL fragments are only scanned here
(config/sqlcheck.py); `dre dry-run` parses them against Spark (config/fragments.py),
and the runtime preconditions (tables and columns exist) are in checks/base.py.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import ValidationError

from hcsc.datalake.dre.checks.patterns import (
    conf_pattern_names, configured_params, is_file_pattern, pattern_causes, pattern_check_ids, pattern_check_params,
    pattern_checks, pattern_file, pattern_kind, probe_keys, TABLE_WIDE,
)
from hcsc.datalake.dre.checks.registry import CHECKS
from hcsc.datalake.dre.checks.rules.template import TEMPLATES
from hcsc.datalake.dre.config.loader import (
    Config, ConfigError, describe, errors_from_validation, format_loc, line_of, load,
)
from hcsc.datalake.dre.config.models import DERIVED_LOAD, DERIVED_RECORD
from hcsc.datalake.dre.config.probes import parameter_sets, PROBES

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

    if defaults is not None and defaults.watchdog_owner is not None and defaults.watchdog_owner not in defaults.recipients:
        err("defaults", "defaults", ("watchdog_owner",), f"watchdog_owner {defaults.watchdog_owner!r} has no recipients",
            f"add `{defaults.watchdog_owner}: [address]` under recipients, or set watchdog_owner: null")

    pattern_errors, broken_patterns = _check_patterns(config)
    errors.extend(pattern_errors)

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
        if feed.pattern in broken_patterns:
            continue  # the pattern file's own error says what is wrong; nothing more can be checked against it
        errors.extend(_check_probes(config, feed_id))
        errors.extend(_check_check_params(config, "feed", feed_id, feed.pattern, feed, None))
        if _is_file_pattern(feed.pattern) and not any(
            datasets[d].layer == "RAW" and datasets[d].file_name_column for d in feed.datasets if d in datasets
        ):
            err("feed", feed_id, ("datasets",),
                "no RAW dataset with a file_name_column: T1_FILES_NOT_LOADED will not run for this feed",
                "add the feed's raw dataset with file_name_column once its columns are confirmed",
                level="warning")

    # Datasets.
    for ds_id, ds in datasets.items():
        key = set(ds.key)

        # Derived datasets (release 2): a query over other datasets, used only as an upstream.
        if ds.derived is not None:
            d = ds.derived
            if ds_id in member_of:
                err("dataset", ds_id, ("derived",), f"feed {member_of[ds_id]!r} lists this derived dataset",
                    "remove it from the feed: a derived dataset is checked only as another dataset's upstream")
            for i, src in enumerate(d.sources):
                loc = ("derived", "from") if i == 0 else ("derived", "joins", i - 1, "dataset")
                if src == ds_id:
                    err("dataset", ds_id, loc, "a derived dataset reads itself", "name another dataset")
                elif src not in datasets:
                    err("dataset", ds_id, loc, f"unknown dataset {src!r}", f"add datasets/{src}.yaml")
                elif datasets[src].derived is not None:
                    err("dataset", ds_id, loc, f"{src!r} is itself derived", "derive from table datasets only")
            base = datasets.get(d.from_)
            if base is not None and base.derived is None:
                for what in ("record_time", "load_time"):
                    if getattr(base, what) is None:
                        err("dataset", d.from_, (what,), f"{what} is not set, and {ds_id!r} derives record_ts and "
                            f"load_ts from it", "add " + what + ": {column: ..., format: ...}")
            for i, c in enumerate(ds.key):
                if c not in d.group_by:
                    err("dataset", ds_id, ("key", i), f"{c!r} is not a group_by output",
                        "a derived dataset's key is its grain: name group_by outputs only")
            for name in d.columns:
                if name in d.group_by:
                    err("dataset", ds_id, ("derived", "columns", name), "named both in group_by and columns",
                        "use each output name once")
            for what, fixed in (("record_time", DERIVED_RECORD), ("load_time", DERIVED_LOAD)):
                col = getattr(ds, what)
                if col is not None and (col.column != fixed.column or col.format is not None
                                        or (col.timezone or "UTC") != "UTC"):
                    err("dataset", ds_id, (what,), f"{what} of a derived dataset is the engine's {fixed.column} (UTC)",
                        f"leave {what} out, or set column: {fixed.column} with no format and timezone UTC")
            for what in ("feed_filter", "file_name_column", "partition_column"):
                if getattr(ds, what) is not None:
                    err("dataset", ds_id, (what,), f"a derived dataset has no {what}",
                        "filter with derived.where; derived datasets have no files or partitions")
            if ds.key_map or ds.upstream or ds.owned_columns:
                err("dataset", ds_id, ("key_map",), "a derived dataset cannot be a downstream (no key_map, upstream or "
                    "owned_columns)", "derive it as an upstream of the dataset that compares against it")
            if feeds_complete and not any(ds_id in other.upstream for other in datasets.values()):
                err("dataset", ds_id, ("dataset",), "no dataset names this derived dataset as an upstream",
                    "add it to the upstream and key_map of the dataset it feeds, or remove it", level="warning")
            continue

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

        feed_of_ds = config.feed_of(ds_id)
        if feed_of_ds is None or feed_of_ds.pattern not in broken_patterns:
            errors.extend(_check_check_params(config, "dataset", ds_id, feed_of_ds.pattern if feed_of_ds else None,
                                              feed_of_ds, ds))

        for col in ds.key_normalise:
            if col not in key:
                err("dataset", ds_id, ("key_normalise", col), f"{col!r} is not a key column",
                    "normalise key columns only")
        for i, col in enumerate(ds.mismatch_probe_drop):
            if col not in key:
                err("dataset", ds_id, ("mismatch_probe_drop", i), f"{col!r} is not a key column",
                    "list key columns only")
        for up_id in ds.hop_exclusions:
            if up_id not in ds.key_map:
                err("dataset", ds_id, ("hop_exclusions", up_id), f"{up_id!r} is not an upstream in key_map",
                    "give exclusions only for the upstreams this dataset's key_map maps")
        if ds.winner_rule is not None:
            ordered = {term.split()[0] for term in ds.winner_rule.order_by}
            for col in ds.winner_rule.formats:
                if col not in ordered:
                    err("dataset", ds_id, ("winner_rule", "formats", col), f"{col!r} is not in winner_rule.order_by",
                        "give formats only for columns the winner rule orders by")
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

    # Rules: dataset, owner, template params, the datasets the template names.

    for rule_id, rule in config.rules.items():
        if rule.dataset not in datasets:
            err("rule", rule_id, ("dataset",), f"unknown dataset {rule.dataset!r}",
                f"add datasets/{rule.dataset}.yaml or point the rule at an existing dataset")
        check_owner("rule", rule_id, rule.owner)
        spec = TEMPLATES[rule.template]
        try:
            params = spec.params_model.model_validate(rule.params)
        except ValidationError as exc:
            source = config.sources[("rule", rule_id)]
            errors.extend(errors_from_validation(exc, source.file, source.node, prefix=("params",)))
            continue
        for field_name, related in spec.related(params).items():
            if related not in datasets:
                err("rule", rule_id, ("params", field_name), f"unknown dataset {related!r}",
                    f"add datasets/{related}.yaml or fix {field_name}")

    return errors


def _is_file_pattern(pattern: str) -> bool:
    try:
        return is_file_pattern(pattern)
    except ValueError:
        return False


def _check_patterns(config: Config) -> tuple[list[ConfigError], set[str]]:
    """Each conf/patterns file loads: `pattern` matches its name, `kind` is set, every check, probe
    type and builtin cause it names is registered, cause lists are well formed. Returns the errors
    and the names of the patterns that failed, so nothing else is checked against those."""

    found = []
    broken: set[str] = set()
    for pattern in sorted(conf_pattern_names()):
        for read in (pattern_kind, pattern_checks, pattern_causes):
            try:
                read(pattern)
            except ValueError as exc:
                found.append(ConfigError(pattern_file(pattern), 1, "", str(exc).replace(pattern_file(pattern) + ": ", ""),
                                         "fix the pattern file; the packaged patterns/*.yaml are the examples"))
                broken.add(pattern)
                break
    return found, broken


def _check_check_params(config: Config, kind: str, obj_id: str, pattern: str | None, feed: Any,
                        ds: Any) -> list[ConfigError]:
    """`check_params` on a feed or a dataset. The object's own entries must name a parametrised check
    its pattern lists. Then every check that will be planned for a dataset (configured at any level:
    pattern defaults, feed, dataset) must validate once merged: a wrong value is reported where it
    is written, a field missing at every level on the dataset."""


    obj = ds if kind == "dataset" else feed
    source = config.sources[(kind, obj_id)]
    found: list[ConfigError] = []
    listed = pattern_check_ids(pattern or TABLE_WIDE)

    def report(loc: tuple[Any, ...], problem: str, fix: str, level: str = "error") -> None:
        file, line = config.locate(kind, obj_id, loc)
        found.append(ConfigError(file, line, format_loc(loc), problem, fix, level))

    usable: list[str] = []
    for check_id in obj.check_params:
        loc = ("check_params", check_id)
        check = CHECKS.get(check_id)
        if check is None:
            report(loc, f"unknown check {check_id!r}",
                   f"use one of {[c for c in CHECKS.ids() if CHECKS[c].params_model is not None]}")
        elif check.params_model is None:
            report(loc, f"{check_id} takes no parameters", "remove it")
        elif check_id not in listed:
            report(loc, f"pattern {pattern or TABLE_WIDE} does not list {check_id}, so it will not run",
                   "remove it, or use a pattern that lists it", level="warning")
        else:
            usable.append(check_id)

    if kind == "feed":
        # the feed's own values, with the pattern's defaults under them: only wrong values are reported here;
        # a field missing at every level is reported on each dataset, where the merge is complete
        for check_id in usable:
            merged = configured_params(CHECKS[check_id], ds_placeholder(), feed, pattern) or {}
            for err in _value_errors(CHECKS[check_id], merged):
                field_name = str(err["loc"][0]) if err["loc"] else ""
                if err["type"] != "missing" and field_name in obj.check_params[check_id]:
                    problem, fix = describe(err)
                    where = ("check_params", check_id, *err["loc"])
                    found.append(ConfigError(source.file, line_of(source.node, where), format_loc(where), problem, fix))
        return found

    planned = set(usable) | {c for c in (feed.check_params if feed else {}) if c in listed and CHECKS.get(c)} \
        | set(pattern_check_params(pattern or TABLE_WIDE))
    for check_id in sorted(planned):
        merged = configured_params(CHECKS[check_id], ds, feed, pattern) or {}
        own = check_id in obj.check_params
        for err in _value_errors(CHECKS[check_id], merged):
            field_name = str(err["loc"][0]) if err["loc"] else ""
            if err["type"] == "missing":
                where_feed = f" in feed {feed.feed}," if feed else ""
                where = ("check_params", check_id, field_name) if own else ("check_params",)
                report(where, f"`{field_name}` is not set at any level" if own
                       else f"check_params.{check_id}: `{field_name}` is not set at any level",
                       f"set it under check_params.{check_id}{where_feed} or in this dataset")
            elif own and (not err["loc"] or field_name in obj.check_params[check_id]):  # a value written here
                problem, fix = describe(err)
                where = ("check_params", check_id, *err["loc"])
                found.append(ConfigError(source.file, line_of(source.node, where), format_loc(where), problem, fix))
    return found


def _value_errors(check: Any, merged: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        check.params_model.model_validate(merged)
    except ValidationError as exc:
        return list(exc.errors())
    return []


def ds_placeholder() -> Any:
    """A dataset with no check_params, for validating a feed's values on their own."""
    from types import SimpleNamespace

    return SimpleNamespace(check_params={})


def _check_probes(config: Config, feed_id: str) -> list[ConfigError]:
    """Each `probes:` key must be one the feed's pattern uses, with parameters for its probe type."""

    feed = config.feeds[feed_id]
    source = config.sources[("feed", feed_id)]
    keys = probe_keys(feed.pattern)
    found: list[ConfigError] = []
    for key, raw in feed.probes.items():
        if key not in keys:
            file, line = config.locate("feed", feed_id, ("probes", key))
            found.append(ConfigError(file, line, f"probes.{key}",
                                     f"pattern {feed.pattern} has no cause that uses probe key {key!r}",
                                     f"use one of {sorted(keys)}, or remove it"))
            continue
        probe = PROBES.get(keys[key])
        if probe is None:
            continue
        model = probe.params_model
        many = isinstance(raw, list)
        for i, params in enumerate(parameter_sets(raw)):
            try:
                model.model_validate(params)
            except ValidationError as exc:
                prefix = ("probes", key, i) if many else ("probes", key)
                found.extend(errors_from_validation(exc, source.file, source.node, prefix=prefix))
    return found


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
