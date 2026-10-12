"""What every cause check shares (spec section 7): outcomes, failures and the context.

A cause check answers one question about one failure: CONFIRMED, RULED_OUT, NOT_READY
(a configuration input it needs is missing) or ERROR (it could not be evaluated). Its
evidence holds times, counts, paths and table names only: never key values or file or
log contents.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable

from hcsc.datalake.dre.checks.base import error_detail
from hcsc.datalake.dre.checks.times import column_timezone, truncate

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from hcsc.datalake.dre.checks.base import CheckContext
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.loader import Config
    from hcsc.datalake.dre.config.models import Dataset, Feed

CONFIRMED, RULED_OUT, NOT_READY, ERROR = "CONFIRMED", "RULED_OUT", "NOT_READY", "ERROR"


@dataclass(frozen=True)
class Outcome:
    state: str
    evidence: dict[str, Any] = field(default_factory=dict)


def confirmed(**evidence: Any) -> Outcome:
    return Outcome(CONFIRMED, evidence)


def ruled_out(**evidence: Any) -> Outcome:
    return Outcome(RULED_OUT, evidence)


def not_ready(reason: str) -> Outcome:
    return Outcome(NOT_READY, {"reason": reason})


def error(exc: BaseException) -> Outcome:
    return Outcome(ERROR, {"error": error_detail(exc)})


@dataclass(frozen=True)
class Failure:
    """One failing thing a check found. ref: what goes in failure_ref (a landed file path, a slot
    in UTC, an upstream file's base name, or a key_hash); None when it cannot be referenced."""

    ref: str | None
    files: tuple[str, ...] | None = None  # landed files concerned; None: unknown or not file-based
    slot: datetime | None = None          # a missed or short load slot (UTC)
    file_name: str | None = None          # an upstream file (base name) short in this dataset
    key: dict[str, Any] | None = None     # hop key facts; key["k"] (the key) never leaves memory


@dataclass
class CauseContext:
    """The failing check's context, plus a per-evaluation cache shared by its cause checks."""

    ctx: CheckContext
    event: Event
    config: Config
    check_id: str
    upstream_id: str | None = None  # the hop's upstream, for hop checks
    cache: dict[str, Any] = field(default_factory=dict)

    @property
    def spark(self) -> SparkSession:
        return self.ctx.spark

    @property
    def dataset(self) -> Dataset:
        return self.ctx.dataset

    @property
    def feed(self) -> Feed | None:
        return self.ctx.feed

    def cached(self, key: str, compute: Callable[[], Any]) -> Any:
        if key not in self.cache:
            self.cache[key] = compute()
        return self.cache[key]

    def upstream(self) -> Dataset | None:
        return self.ctx.upstreams.get(self.upstream_id) if self.upstream_id else None

    def raw_dataset(self) -> Dataset | None:
        """The feed's RAW dataset with a file_name_column, which landed files load into."""
        if self.feed is None:
            return None
        for ds_id in self.feed.datasets:
            ds = self.config.datasets.get(ds_id)
            if ds is not None and ds.layer == "RAW" and ds.file_name_column is not None:
                return ds
        return None

    def at_load_granularity(self, value: datetime) -> datetime:
        """A time truncated to this dataset's load_time granularity, in the load time's zone."""
        load_time = self.dataset.load_time
        return truncate(value, load_time.granularity, column_timezone(load_time, self.ctx.default_timezone))
