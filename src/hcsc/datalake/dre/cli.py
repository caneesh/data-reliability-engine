"""Command line entry point for `dre` (spec section 8).

`validate` (step 2) and `install` (step 3) are built; `run` checks the store
is installed. Each other subcommand is filled in by the build step named in
its help text (spec section 10) and until then says so.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from hcsc.datalake.dre import __version__

# Subcommand name -> (help text, build step that implements it).
COMMANDS: dict[str, tuple[str, int]] = {
    "validate": ("Static validation of all config", 2),
    "install": ("Print, apply or check the dq store tables and views", 3),
    "dry-run": ("Run preconditions and checks for one feed; write nothing", 4),
    "run": ("Full run: preconditions, checks, causes, results, email", 8),
    "trace": ("Trace one key along the upstream chain", 9),
    "watchdog": ("Check the last expected run exists and is complete", 10),
}
CONF_HELP = "configuration directory (default: conf)"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dre", description="Data Reliability Engine")
    parser.add_argument("--version", action="version", version=f"dre {__version__}")
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    for name, (help_text, step) in COMMANDS.items():
        cmd = sub.add_parser(name, help=f"{help_text} (build step {step})")
        if name in ("validate", "install", "run"):
            cmd.add_argument("--conf", default="conf", help=CONF_HELP)
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


def run(conf: str) -> int:
    """Exit 3 when the run cannot start: invalid config or the store not installed."""
    from hcsc.datalake.dre.config.validate import validate_conf
    from hcsc.datalake.dre.session import get_spark
    from hcsc.datalake.dre.store.install import missing_objects

    config, errors, _ = validate_conf(Path(conf))
    if errors or config.defaults is None:
        print(f"dre run: {conf} has {len(errors)} configuration error(s); run dre validate --conf {conf}")
        return 3
    db = config.defaults.dq_database
    missing = missing_objects(get_spark(), db)
    if missing:
        print(f"dre run: the dq store is not installed in {db} (missing: {', '.join(missing)}). "
              "Run dre install --apply once the platform team has created the database.")
        return 3
    _, step = COMMANDS["run"]
    print(f"dre run: store ready in {db}; checks are not built yet (build step {step})", file=sys.stderr)
    return 3


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
    if args.command == "run":
        return run(args.conf)
    _, step = COMMANDS[args.command]
    print(f"dre {args.command}: not built yet (build step {step})", file=sys.stderr)
    return 3


if __name__ == "__main__":
    sys.exit(main())
