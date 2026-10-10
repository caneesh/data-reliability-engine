"""The failing things of a FAILED check, each explained on its own (spec section 7).

- T1_FILES_NOT_LOADED: each not-loaded file (from `detail`).
- T1_ON_TIME: each missed slot, with the files first seen since the slot that have no raw rows.
- T1_ZERO_ROWS: each short slot.
- HOP_FILE_COMPLETENESS: each short upstream file (base name, from `detail`).
- HOP_KEY_CURRENCY and HOP_VALUE_AGREEMENT: each failing key, by key_hash. Without an HMAC
  secret keys cannot be referenced: one failure with no reference, whose causes are NOT_READY.
At most LISTED failures per evaluation, as in check details.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING

from hcsc.datalake.dre.causes.base import CauseContext, Failure
from hcsc.datalake.dre.causes.landing import unloaded_files_after
from hcsc.datalake.dre.causes.merge import hop
from hcsc.datalake.dre.checks.base import render_sql
from hcsc.datalake.dre.checks.times import as_utc

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.base import CheckResult

LISTED = 100
KEY_CHECKS = ("HOP_KEY_CURRENCY", "HOP_VALUE_AGREEMENT")


def failures_for(cx: CauseContext, result: CheckResult) -> list[Failure]:
    detail = json.loads(result.detail) if result.detail and result.detail.startswith("{") else {}
    check = cx.check_id
    if check == "T1_FILES_NOT_LOADED":
        return [Failure(ref=path, files=(path,)) for path in detail.get("not_loaded", [])[:LISTED]]
    if check in ("T1_ON_TIME", "T1_ZERO_ROWS"):
        slots = [as_utc(datetime.fromisoformat(s)) for s in detail.get("slots", [])[:LISTED]]
        if check == "T1_ZERO_ROWS":
            return [Failure(ref=s.isoformat(), slot=s) for s in slots]
        return [Failure(ref=s.isoformat(), slot=s, files=unloaded_files_after(cx, s)) for s in slots]
    if check == "HOP_FILE_COMPLETENESS":
        return [Failure(ref=f["file"], file_name=f["file"]) for f in detail.get("short_files", [])[:LISTED]]
    if check in KEY_CHECKS:
        if cx.ctx.key_secret is None:
            return [Failure(ref=None)]
        mode = "currency" if check == "HOP_KEY_CURRENCY" else "agreement"
        rows = cx.spark.sql(render_sql("hop_key_failures.sql.j2", p=hop(cx), mode=mode, limit=LISTED)).collect()
        failures = [Failure(ref=r.key_hash, key=r.asDict()) for r in rows]
        cx.cache["failure_hashes"] = [f.ref for f in failures]
        return failures
    return []
