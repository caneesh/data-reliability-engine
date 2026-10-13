"""Notification sinks (spec section 8): `notify:` names them, the email block alone still means one
email sink, a sink registers from its own module, and a failing sink never stops a run."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

import pytest

from hcsc.datalake.dre.config.loader import load
from hcsc.datalake.dre.config.models import Defaults, Model
from hcsc.datalake.dre.config.validate import validate_conf
from hcsc.datalake.dre.notify.sinks import (
    SINKS, EmailSink, Message, SinkKind, StdoutSink, deliver, sinks_for, validate_sink_config,
)
from tests.config.conftest import SAMPLE_CONF, edit, line_with

MESSAGES = [Message("membership-gold", "DRE test: all clear", "nothing to report", ["a@example.org"])]


@pytest.fixture
def conf(tmp_path: Path) -> Path:
    target = tmp_path / "conf"
    shutil.copytree(SAMPLE_CONF, target)
    return target


def test_the_packaged_sinks() -> None:
    assert SINKS.ids() == ("email", "stdout")


def test_without_notify_the_email_block_is_one_email_sink(conf: Path) -> None:
    config, errors = load(conf)
    assert errors == []
    [only] = sinks_for(config.defaults)
    assert isinstance(only, EmailSink) and only.server.smtp_port == 25 and only.server.smtp_host is None
    assert deliver(MESSAGES, config.defaults) == [
        "email for membership-gold: DRE test: all clear (not sent: email.smtp_host and email.sender are not set)"]


def test_notify_lists_sinks_in_order(conf: Path, capsys) -> None:
    (conf / "defaults.yaml").write_text((conf / "defaults.yaml").read_text() +
                                        "notify:\n  - { kind: stdout }\n  - { kind: email, smtp_host: relay.example.org, "
                                        "smtp_port: 2525, sender: dre@example.org }\n")
    config, errors = load(conf)
    assert errors == [], "\n".join(map(str, errors))
    kinds = [s.kind for s in sinks_for(config.defaults)]
    assert kinds == ["stdout", "email"]
    assert sinks_for(config.defaults)[1].server.smtp_port == 2525
    lines = deliver(MESSAGES, config.defaults)
    out = capsys.readouterr().out
    assert "--- DRE test: all clear (to 1 for membership-gold)\nnothing to report" in out
    assert lines[0] == "stdout for membership-gold: DRE test: all clear (printed)"
    assert lines[1].startswith("email for membership-gold: not sent (")  # the relay here does not resolve


CASES = [
    ("unknown-kind", "notify:\n  - { kind: pigeon }\n", "unknown sink kind 'pigeon'", "email"),
    ("bad-field", "notify:\n  - { kind: email, smtp_port: soon }\n", "smtp_port", "integer"),
    ("not-a-mapping", "notify:\n  - email\n", "is a mapping with a kind", "stdout"),
    ("bad-sender", "notify:\n  - { kind: email, sender: nobody }\n", "sender", "email address"),
]


@pytest.mark.parametrize(("case", "block", "problem", "fix"), CASES, ids=[c[0] for c in CASES])
def test_notify_errors_name_file_line_and_problem(conf: Path, case, block, problem, fix) -> None:
    (conf / "defaults.yaml").write_text((conf / "defaults.yaml").read_text() + block)
    _, errors, _ = validate_conf(conf)
    [e] = [e for e in errors if e.field.startswith("notify")]
    assert problem in e.problem and fix in e.fix + e.problem
    assert e.line >= line_with(conf, "defaults.yaml", "notify:")


class PigeonConfig(Model):
    kind: str
    loft: str


@dataclass
class Pigeon:
    loft: str
    kind: str = "pigeon"
    flown: list = None  # type: ignore[assignment]

    def send(self, messages: list[Message]) -> list[str]:
        self.flown = list(messages)
        return [f"pigeon for {m.owner}: {m.subject} (flown to {self.loft})" for m in messages]


def test_a_sink_registers_from_its_own_module_and_a_failing_sink_is_reported() -> None:
    pigeon = Pigeon("roof")
    with SINKS.temporary("pigeon", SinkKind("pigeon", PigeonConfig, lambda c: pigeon)):
        cfg = validate_sink_config({"kind": "pigeon", "loft": "roof"})
        assert isinstance(cfg, PigeonConfig)
        defaults = Defaults.model_validate({**_minimal_defaults(), "notify": [{"kind": "pigeon", "loft": "roof"}]})
        assert deliver(MESSAGES, defaults) == ["pigeon for membership-gold: DRE test: all clear (flown to roof)"]
        assert pigeon.flown == MESSAGES

    class Broken:
        kind = "broken"

        def send(self, messages):
            raise RuntimeError("the body would be in this message")

    with SINKS.temporary("broken", SinkKind("broken", StdoutSinkConfigAlias, lambda c: Broken())):
        defaults = Defaults.model_validate({**_minimal_defaults(), "notify": [{"kind": "broken"}]})
        assert deliver(MESSAGES, defaults) == ["broken: not sent (RuntimeError)"]  # the message text never leaks


class StdoutSinkConfigAlias(Model):
    kind: str


def _minimal_defaults() -> dict:
    import yaml

    return yaml.safe_load((SAMPLE_CONF / "defaults.yaml").read_text(encoding="utf-8"))


def test_stdout_prints_counts_unless_asked_for_the_body(capsys) -> None:
    """A11: the run's stdout is a log; a digest prints as its subject and counts unless full_body."""
    digest_like = Message("o", "DRE dev: 1 new failures, 0 checks did not run", "src_sys_nm=SRC_A: HOP_KEY_CURRENCY FAILED",
                          ["a@example.org"], summary="1 new failures, 0 checks did not run, 0 still failing, 3 passing")
    assert StdoutSink().send([digest_like]) == ["stdout for o: DRE dev: 1 new failures, 0 checks did not run (printed)"]
    out = capsys.readouterr().out
    assert "3 passing" in out and "src_sys_nm=SRC_A" not in out
    StdoutSink(full_body=True).send([digest_like])
    assert "src_sys_nm=SRC_A" in capsys.readouterr().out
    StdoutSink().send([Message("watchdog", "DRE dev: watchdog", "run r1 is still STARTED", ["a@example.org"])])
    assert "run r1 is still STARTED" in capsys.readouterr().out  # no summary: the body is counts and ids already
    assert validate_sink_config({"kind": "stdout", "full_body": True}).full_body is True


def test_digests_carry_a_counts_summary_for_log_sinks() -> None:
    from datetime import datetime, timezone

    from hcsc.datalake.dre.notify.digest import Digest, Item
    from hcsc.datalake.dre.notify.sinks import from_digests

    digest = Digest("o", ["a@example.org"], "dev", "r1", datetime(2026, 1, 1, tzinfo=timezone.utc), passing=3,
                    did_not_run=[Item("ds", "T1_ON_TIME", "DID_NOT_RUN", "", "", "", "")])
    [message] = from_digests([digest])
    assert message.summary.startswith("0 new failures, 1 checks did not run, 0 still failing, 3 passing")
