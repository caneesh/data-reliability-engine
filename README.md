# data-reliability-engine

The Data Reliability Engine (DRE) is a read-only engine for data quality checks on the data lake. It checks feeds and tables against YAML configuration, writes every result to its own append-only `dq` store, works out the cause of each failure, and emails a digest.

- Build spec: [`docs/spec.md`](docs/spec.md) (source of truth)
- Working rules for Claude Code: [`CLAUDE.md`](CLAUDE.md)
- Open decisions and defaults taken: [`docs/decisions.md`](docs/decisions.md)

## Development

Needs Python 3.10+ and Java 17 for local-mode Spark.

```bash
pip install -e ".[dev]"
pytest                 # all tests, local-mode Spark
pytest tests/guard     # guard tests only
```

Status: build step 2 (configuration) of spec section 10. `dre validate --conf conf/` checks the sample configuration in `conf/`; the other `dre` subcommands are placeholders until their build steps land.
