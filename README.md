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

Release 1 is built (spec section 10, steps 1 to 11). Commands: `dre validate`, `dre install --print|--apply|--check`, `dre run [--feed F]`, `dre dry-run --feed F`, `dre trace --dataset D --key "k1,k2,..."` and `dre watchdog`, each with `--conf DIR`.

## Deploying with spark-submit

Build the release artifacts (needs network for the dependency wheels; pick the cluster's Python and platform):

```bash
python scripts/package.py --python-version 3.10 --platform manylinux2014_x86_64
```

`dist/` then holds:

| File | What it is for |
| --- | --- |
| `hcsc_datalake_dre-<version>-py3-none-any.whl` | The package as a wheel |
| `dre-pyfiles-<version>.zip` | The package for `--py-files`: executors import it for the key-hash function |
| `dre-deps-<version>-<platform>.zip` | Wheels of the dependencies other than PySpark (pydantic, PyYAML, Jinja2), for an offline install |
| `dre_main.py` | The `spark-submit` entry point; its arguments are `dre`'s |

On the driver host, install the dependencies once into the Python that Spark uses (`PYSPARK_PYTHON`). pydantic has a compiled core, so it is installed, not shipped in `--py-files`; run with `--deploy-mode client` so the driver uses this Python.

```bash
mkdir wheels && unzip dre-deps-<version>-<platform>.zip -d wheels
python -m pip install --no-index --find-links wheels pydantic PyYAML Jinja2
```

Create the store once (the platform team creates the empty database and grants the service account write access to it only):

```bash
SUBMIT="spark-submit --master yarn --deploy-mode client --py-files dre-pyfiles-<version>.zip dre_main.py"
$SUBMIT validate --conf /path/to/conf
$SUBMIT install --conf /path/to/conf --apply
```

Schedule the run every hour, and the watchdog every hour from a different scheduler folder or host:

```bash
$SUBMIT run --conf /path/to/conf        # exit 0 completed, 2 partial, 3 could not start
$SUBMIT watchdog --conf /path/to/conf   # exit 0 healthy, 1 alert raised
```

Trace one key on demand: `$SUBMIT trace --conf /path/to/conf --dataset gold_member_coverage --key "k1,k2,k3,k4"`.

The configuration directory is the deployed instance's own (real tables, paths, recipients, the mail relay and the HMAC secret file); `conf/` in this repository is a synthetic sample.

## Settings that carry an assumption

Where the spec left a choice open, the engine takes a default and makes it a setting, so an answer is a configuration change. All are in `defaults.yaml` unless noted; the reasons are in `docs/decisions.md`.

| Setting | Default | What it decides |
| --- | --- | --- |
| `email.smtp_host`, `email.smtp_port`, `email.sender` | null, 25, null | The mail relay; null: digests and watchdog alerts are printed, not sent |
| `email.all_clear_digest` | `every_run` | An owner's "all clear" digest every run, `daily` (the run in the hour from `rule_run_at`) or `never`; digests with problems are always sent |
| `watchdog_grace_minutes` | 30 | How long after the hour the watchdog expects that hour's run |
| `watchdog_owner` | null | Whose recipients get watchdog alerts; null: every address in `recipients` |
| `compute_budget_scope` | `check` | `check`: each check gets `compute_budget_minutes`; `dataset`: a dataset's checks share it in a run |
| `scan_mb_per_minute` | null | Cluster throughput; when set, a check whose estimated scan exceeds its budget is refused before running |
| `max_window_hours` | 72 | How long a PLATFORM or BUDGET problem holds a window before it is given up as a WINDOW_GAP |
| `rule_run_at`, `rule_weekly_day` | `"06:00"`, null | When daily and weekly gold rules fall due (null: weekly on each dataset's `full_sweep_day`) |
| Probe `file_value_compare.confirm_when` (feed `probes:`) | `later` | WRONG_PARTITION and SKIPPED_BEHIND_CURSOR confirm when the cursor's partition is `later`, or any `different` one |
| Probe `file_value_compare.format` (feed `probes:`) | null | The cursor file's date format (a Spark pattern); null: the probe is NOT_READY |

Production values still unknown (table and column names, paths, time formats, filter rules) stay `null` in configuration until confirmed; the checks and causes that need them report DID_NOT_RUN or NOT_READY, never a guess.
