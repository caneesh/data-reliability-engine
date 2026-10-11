"""Command line entry point for `dre` (spec section 8).

Subcommands: validate, install, dry-run, run, trace, watchdog and retention.
"""

from __future__ import annotations

import argparse
import sys
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
}
CONF_HELP = "configuration directory (default: conf)"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dre", description="Data Reliability Engine")
    parser.add_argument("--version", action="version", version=f"dre {__version__}")
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    for name, help_text in COMMANDS.items():
        cmd = sub.add_parser(name, help=help_text)
        cmd.add_argument("--conf", default="conf", help=CONF_HELP)
        if name == "dry-run":
            cmd.add_argument("--feed", required=True, help="the feed to check")
        if name == "trace":
            cmd.add_argument("--dataset", required=True, help="the dataset to start from")
            cmd.add_argument("--key", required=True, help='the key values in the dataset\'s key order, "k1,k2,..."')
        if name == "run":
            cmd.add_argument("--feed", help="run one feed only (default: all feeds)")
            cmd.add_argument("--execution-type", default="NORMAL", choices=("NORMAL", "RERUN", "REPLAY"))
        if name == "retention":
            cmd.add_argument("--apply", action="store_true", help="drop the expired partitions (default: list them)")
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
    print(
        f"dre validate: {conf} is valid, {len(warnings)} warning(s) "
        f"({len(config.feeds)} feeds, {len(config.datasets)} datasets, {len(config.rules)} rules)"
    )
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


def main(argv: list[str] | None = None) -> int:
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
            return runner.run(get_spark(), args.conf, args.feed, args.execution_type)
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
    parser.error(f"unknown command {args.command}")  # argparse rejects these first
    return 2


if __name__ == "__main__":
    sys.exit(main())
