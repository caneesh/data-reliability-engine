"""List landing folders through the Hadoop FileSystem API (spec sections 1 and 2).

Read-only: it lists and reads file status, nothing else. The same code lists
HDFS on the cluster and the local filesystem in tests (file:// paths). Hidden
files (names starting with `.` or `_`, such as `_SUCCESS` or `.crc`) are skipped,
as Hadoop tools do.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from fnmatch import fnmatchcase
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# Java exception names that mean the root cannot be read (as opposed to HDFS being down).
_UNREADABLE = ("FileNotFoundException", "AccessControlException", "PathNotFoundException")


@dataclass(frozen=True)
class FileInfo:
    path: str
    size_bytes: int
    modified_at: datetime  # UTC


class LandingError(Exception):
    """A landing root could not be listed. code: landing_unreadable or hdfs_unavailable."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


def _code(exc: Exception) -> str:
    text = f"{type(exc).__name__} {getattr(exc, 'java_exception', '')}"
    return "landing_unreadable" if any(name in text for name in _UNREADABLE) else "hdfs_unavailable"


def list_landing(spark: SparkSession, roots: list[str], pattern: str = "*") -> list[FileInfo]:
    """Every file under the roots (recursively) whose name matches pattern, sorted by path."""
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    found: list[FileInfo] = []
    for root in roots:
        try:
            path = jvm.org.apache.hadoop.fs.Path(root)
            fs = path.getFileSystem(conf)
            if not fs.exists(path):
                raise LandingError("landing_unreadable", f"landing root {root} not found")
            files = fs.listFiles(path, True)
            while files.hasNext():
                status = files.next()
                name = status.getPath().getName()
                if name.startswith((".", "_")) or not fnmatchcase(name, pattern):
                    continue
                found.append(FileInfo(
                    path=status.getPath().toString(),
                    size_bytes=int(status.getLen()),
                    modified_at=datetime.fromtimestamp(status.getModificationTime() / 1000, tz=timezone.utc),
                ))
        except LandingError:
            raise
        except Exception as exc:
            raise LandingError(_code(exc), f"cannot list landing root {root}: {type(exc).__name__}") from None
    return sorted(found, key=lambda f: f.path)
