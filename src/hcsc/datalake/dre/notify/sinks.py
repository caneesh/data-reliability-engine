"""Notification sinks (spec section 8): where digests and watchdog alerts go.

A sink takes messages (owner, subject, body, recipients) and returns one status line each. The
packaged sinks are `email` (SMTP, notify/email.py) and `stdout` (printed: what an environment
without a mail relay gets). `defaults.yaml` names them under `notify:`; with none listed, the
`email:` block stands for one email sink, as before. Sinks are a registry like the other
capabilities, so a webhook sink is a module that registers itself. Every message carries counts,
check ids and table names only: hard rule 6 applies to every sink, and the guard that nothing
in notify/ references the key column of dq_key_event covers them all.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Literal, Protocol

from hcsc.datalake.dre.config.models import Email, EmailServer, Model, NonEmptyStr
from hcsc.datalake.dre.registry import Registry

if TYPE_CHECKING:
    from hcsc.datalake.dre.config.models import Defaults
    from hcsc.datalake.dre.notify.digest import Digest

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Message:
    owner: str
    subject: str
    body: str
    recipients: list[str]


class Sink(Protocol):
    kind: str

    def send(self, messages: list[Message]) -> list[str]:
        """One status line per message; never raises for one message's failure."""


@dataclass(frozen=True)
class SinkKind:
    kind: str
    config: type[Model]  # the `notify:` entry's fields, `kind` included
    build: Callable[[Any], Sink]


SINKS: Registry[SinkKind] = Registry("notification sink")


def sink(kind: str, config: type[Model]) -> Callable[[Callable[[Any], Sink]], Callable[[Any], Sink]]:
    """Register the decorated factory as the sink kind `kind`, configured by `config`."""
    def register(build: Callable[[Any], Sink]) -> Callable[[Any], Sink]:
        SINKS.register(kind, SinkKind(kind, config, build))
        return build
    return register


# --- the packaged sinks ---


class EmailSinkConfig(Model):
    kind: Literal["email"]
    smtp_host: NonEmptyStr | None = None
    smtp_port: int = 25
    sender: Email | None = None


@dataclass(frozen=True)
class EmailSink:
    server: EmailServer
    kind: str = "email"

    def send(self, messages: list[Message]) -> list[str]:
        from hcsc.datalake.dre.notify.email import send_text

        return [send_text(m.owner, m.subject, m.body, m.recipients, self.server) for m in messages]


@sink("email", EmailSinkConfig)
def _email(config: EmailSinkConfig) -> Sink:
    return EmailSink(EmailServer(smtp_host=config.smtp_host, smtp_port=config.smtp_port, sender=config.sender))


class StdoutSinkConfig(Model):
    kind: Literal["stdout"]


@dataclass(frozen=True)
class StdoutSink:
    kind: str = "stdout"

    def send(self, messages: list[Message]) -> list[str]:
        for m in messages:
            print(f"--- {m.subject} (to {len(m.recipients)} for {m.owner})\n{m.body}")
        return [f"stdout for {m.owner}: {m.subject} (printed)" for m in messages]


@sink("stdout", StdoutSinkConfig)
def _stdout(config: StdoutSinkConfig) -> Sink:
    return StdoutSink()


# --- configuration and delivery ---


def validate_sink_config(raw: Any) -> Model:
    """A `notify:` entry, validated by its kind's model; raises ValueError with a readable message."""
    if not isinstance(raw, dict) or not isinstance(raw.get("kind"), str):
        raise ValueError(f"a notify entry is a mapping with a kind; use one of {list(SINKS.ids())}")
    found = SINKS.get(raw["kind"])
    if found is None:
        raise ValueError(f"unknown sink kind {raw['kind']!r}; use one of {list(SINKS.ids())}")
    from pydantic import ValidationError

    try:
        return found.config.model_validate(raw)
    except ValidationError as exc:
        problems = "; ".join(f"{'.'.join(map(str, e['loc'])) or 'entry'}: {e['msg']}" for e in exc.errors())
        raise ValueError(f"{raw['kind']} sink: {problems}") from None


def sinks_for(defaults: Defaults) -> list[Sink]:
    """The configured sinks: `notify:` entries, or one email sink from `email:` when none are listed."""
    if not defaults.notify:
        return [EmailSink(defaults.email)]
    return [SINKS[c.kind].build(c) for c in defaults.notify]


def deliver(messages: list[Message], defaults: Defaults) -> list[str]:
    """Send through every sink; a sink that raises is reported (exception type only), never re-raised."""
    lines: list[str] = []
    for target in sinks_for(defaults):
        try:
            lines += target.send(messages)
        except Exception as exc:
            log.error("sink %s failed: %s", target.kind, type(exc).__name__)  # the type only: hard rule 6
            lines.append(f"{target.kind}: not sent ({type(exc).__name__})")
    return lines


def from_digests(digests: list[Digest]) -> list[Message]:
    return [Message(d.owner, d.subject, d.text(), d.recipients) for d in digests]
