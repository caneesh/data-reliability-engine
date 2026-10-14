"""Command line entry point for `dre` (spec section 8).

Subcommands: validate, install, dry-run, run, trace, watchdog and retention.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from hcsc.datalake.dre import __version__

# Subcommand name -> help text.
COMMANDS: dict[str, str] = {
    "validate": "Static validation of all config",
    "install": "Print, apply or check the dq store tables and views",
    "dry-run": "Run preconditions and checks for one feed; write nothing",
    "run": "Full run: preconditions, checks, causes, results, email",
    "trace": "Trace one key along the upstream chain",
    "watchdog": "Check the last expected run exists and is complete",
    "retention": "List (or with --apply drop) dq store partitions older than retention_months",
    "guard": "Scan a source tree (an extension package) for writes the engine's rules forbid",
}
CONF_HELP = "configuration directory (default: conf)"


def utc_time(text: str) -> datetime:
    """An ISO 8601 time; without an offset it is UTC."""
    try:
        value = datetime.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{text!r} is not an ISO 8601 time, e.g. 2026-01-15T06:00") from None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dre", description="Data Reliability Engine")
    parser.add_argument("--version", action="version", version=f"dre {__version__}")
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    for name, help_text in COMMANDS.items():
        cmd = sub.add_parser(name, help=help_text)
        if name != "guard":
            cmd.add_argument("--conf", default="conf", help=CONF_HELP)
        if name == "dry-run":
            cmd.add_argument("--feed", required=True, help="the feed to check")
        if name == "trace":
            cmd.add_argument("--dataset", required=True, help="the dataset to start from")
            cmd.add_argument("--key", required=True, help='the key values in the dataset\'s key order, "k1,k2,..."')
        if name == "run":
            cmd.add_argument("--feed", help="run one feed only (default: all feeds)")
            cmd.add_argument("--execution-type", default="NORMAL", choices=("NORMAL", "RERUN", "REPLAY"),
                             help="NORMAL: the next eligible windows (scheduled); RERUN: one historical window; "
                                  "REPLAY: every window of a historical range")
            cmd.add_argument("--window-start", type=utc_time, help="RERUN: the window's start (ISO 8601, UTC)")
            cmd.add_argument("--window-end", type=utc_time, help="RERUN: the window's end, excluded")
            cmd.add_argument("--from", dest="range_from", type=utc_time, help="REPLAY: the range's start")
            cmd.add_argument("--to", dest="range_to", type=utc_time, help="REPLAY: the range's end, excluded")
        if name == "retention":
            cmd.add_argument("--apply", action="store_true", help="drop the expired partitions (default: list them)")
        if name == "guard":
            cmd.add_argument("path", help="the directory (or file) to scan")
        if name == "install":
            mode = cmd.add_mutually_exclusive_group(required=True)
            mode.add_argument("--print", dest="mode", action="store_const", const="print",
                              help="print the DDL for the configured dq database")
            mode.add_argument("--apply", dest="mode", action="store_const", const="apply",
                              help="create missing tables and views (safe to rerun)")
            mode.add_argument("--check", dest="mode", action="store_const", const="check",
                              help="compare the existing tables and views with the DDL")
    return parser


def validate(conf: str) -> int:
    from hcsc.datalake.dre.config.validate import validate_conf

    config, errors, warnings = validate_conf(Path(conf))
    for issue in sorted([*errors, *warnings], key=lambda e: (e.file, e.line)):
        print(issue)
    if errors:
        print(f"dre validate: {len(errors)} error(s), {len(warnings)} warning(s) in {conf}")
        return 1
    from hcsc.datalake.dre.checks.patterns import pattern_origin, patterns

    print(
        f"dre validate: {conf} is valid, {len(warnings)} warning(s) "
        f"({len(config.feeds)} feeds, {len(config.datasets)} datasets, {len(config.rules)} rules)"
    )
    by_origin = {o: [p for p in patterns() if pattern_origin(p) == o] for o in ("packaged", "conf")}
    print(f"patterns: packaged {by_origin['packaged']}" + (f", from {conf}/patterns {by_origin['conf']}"
                                                            if by_origin["conf"] else ""))
    from hcsc.datalake.dre.extensions import loaded

    for record in loaded():
        print(f"extensions: {record.summary()}")
    return 0


def install(conf: str, mode: str) -> int:
    from hcsc.datalake.dre.config.loader import load
    from hcsc.datalake.dre.store.install import install_command

    config, errors = load(Path(conf))
    if config.defaults is None:
        for error in errors:
            print(error)
        print(f"dre install: cannot read the dq database name from {conf}/defaults.yaml")
        return 3
    if mode == "print":
        return install_command(None, config.defaults.dq_database, mode)

    from hcsc.datalake.dre.session import get_spark

    return install_command(get_spark(), config.defaults.dq_database, mode)


def retention(conf: str, apply: bool) -> int:
    from datetime import datetime, timezone

    from hcsc.datalake.dre.config.loader import load
    from hcsc.datalake.dre.session import get_spark
    from hcsc.datalake.dre.store.retention import retention_command

    config, errors = load(Path(conf))
    if config.defaults is None:
        for error in errors:
            print(error)
        print(f"dre retention: cannot read retention_months from {conf}/defaults.yaml")
        return 3
    d = config.defaults
    return retention_command(get_spark(), d.dq_database, d.retention_months, apply,
                             datetime.now(timezone.utc).date())


def configure_logging() -> None:
    """The engine's log lines (errors and warnings) to stderr with a time and a level, once per
    process, unless the host configured logging already."""
    import logging

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.WARNING, stream=sys.stderr,
                            format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0
    if args.command == "validate":
        return validate(args.conf)
    if args.command == "install":
        return install(args.conf, args.mode)
    if args.command in ("run", "dry-run"):
        from hcsc.datalake.dre import runner
        from hcsc.datalake.dre.session import get_spark

        if args.command == "run":
            start, end = (args.range_from, args.range_to) if args.execution_type == "REPLAY" else (
                args.window_start, args.window_end)
            if args.execution_type != "REPLAY" and (args.range_from or args.range_to):
                print("dre run: --from and --to are for REPLAY; RERUN takes --window-start and --window-end")
                return runner.EXIT_CANNOT_START
            if args.execution_type == "REPLAY" and (args.window_start or args.window_end):
                print("dre run: REPLAY takes --from and --to; --window-start and --window-end are for RERUN")
                return runner.EXIT_CANNOT_START
            selection = runner.Selection(args.execution_type, start, end)
            return runner.run(get_spark(), args.conf, args.feed, selection)
        return runner.dry_run(get_spark(), args.conf, args.feed)
    if args.command == "trace":
        from hcsc.datalake.dre.session import get_spark
        from hcsc.datalake.dre.trace import run_trace

        return run_trace(get_spark(), args.conf, args.dataset, args.key)
    if args.command == "watchdog":
        from hcsc.datalake.dre.session import get_spark
        from hcsc.datalake.dre.watchdog import watch

        return watch(get_spark("dre-watchdog"), args.conf)
    if args.command == "retention":
        return retention(args.conf, args.apply)
    if args.command == "guard":
        from hcsc.datalake.dre.guard import guard_command

        return guard_command(Path(args.path))
    parser.error(f"unknown command {args.command}")  # argparse rejects these first
    return 2


if __name__ == "__main__":
    sys.exit(main())
