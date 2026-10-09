"""Command line entry point for `dre`.

Build step 1 provides the command surface only. Each subcommand is filled in
by the build step named in its help text (spec section 10).
"""

from __future__ import annotations

import argparse
import sys

from hcsc.datalake.dre import __version__

# Subcommand name -> (help text, build step that implements it).
COMMANDS: dict[str, tuple[str, int]] = {
    "validate": ("Static validation of all config", 2),
    "dry-run": ("Run preconditions and checks for one feed; write nothing", 4),
    "run": ("Full run: preconditions, checks, causes, results, email", 8),
    "trace": ("Trace one key along the upstream chain", 9),
    "watchdog": ("Check the last expected run exists and is complete", 10),
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dre", description="Data Reliability Engine")
    parser.add_argument("--version", action="version", version=f"dre {__version__}")
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    for name, (help_text, step) in COMMANDS.items():
        sub.add_parser(name, help=f"{help_text} (build step {step})")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0
    _, step = COMMANDS[args.command]
    print(f"dre {args.command}: not built yet (build step {step})", file=sys.stderr)
    return 3


if __name__ == "__main__":
    sys.exit(main())
