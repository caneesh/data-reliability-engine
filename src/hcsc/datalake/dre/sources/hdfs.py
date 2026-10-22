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


def list_landing(spark: SparkSession, roots: list[str], pattern: str = "*",
                 max_files: int | None = None) -> list[FileInfo]:
    """Every file under the roots (recursively) whose name matches pattern, sorted by path. More
    than max_files of them is a LandingError (landing_unreadable) naming the count."""
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
                if max_files is not None and len(found) > max_files:
                    raise LandingError("landing_unreadable", f"more than {max_files} files under the landing roots "
                                       "(landing.max_files): archive or raise the limit")
        except LandingError:
            raise
        except Exception as exc:
            raise LandingError(_code(exc), f"cannot list landing root {root}: {type(exc).__name__}") from None
    return sorted(found, key=lambda f: f.path)


@dataclass(frozen=True)
class DirSummary:
    """What a directory's files look like, without reading them (T1_PARTITION_DRIFT)."""

    file_count: int
    total_bytes: int
    latest_modified_at: datetime | None  # UTC; None when the directory holds no files


def summarise_directory(spark: SparkSession, path_text: str) -> DirSummary:
    """File count, total bytes and latest modification time of the files under the path
    (recursively, hidden files skipped). A path that cannot be listed is a LandingError."""
    try:
        path, fs = _fs(spark, path_text)
        if not fs.exists(path):
            raise LandingError("hdfs_unavailable", "partition directory not found")
        files = fs.listFiles(path, True)
        count, total, latest = 0, 0, None
        while files.hasNext():
            status = files.next()
            if status.getPath().getName().startswith((".", "_")):
                continue
            count += 1
            total += int(status.getLen())
            modified = datetime.fromtimestamp(status.getModificationTime() / 1000, tz=timezone.utc)
            latest = modified if latest is None or modified > latest else latest
        return DirSummary(count, total, latest)
    except LandingError:
        raise
    except Exception as exc:
        raise LandingError(_code(exc), f"cannot list a partition directory: {type(exc).__name__}") from None


# Read-only helpers for cause probes (spec section 7): existence and glob. Probes read
# text files with spark.read.text.

def _fs(spark: SparkSession, path_text: str):
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(path_text)
    return path, path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())


def path_exists(spark: SparkSession, path_text: str) -> bool:
    path, fs = _fs(spark, path_text)
    return bool(fs.exists(path))


def glob_files(spark: SparkSession, pattern: str) -> list[str]:
    """Files (not directories) matching a glob, sorted."""
    path, fs = _fs(spark, pattern)
    statuses = fs.globStatus(path) or []
    return sorted(s.getPath().toString() for s in statuses if s.isFile())
