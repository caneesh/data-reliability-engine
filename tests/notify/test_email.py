"""Sending digests: not sent without a mail relay; sent through SMTP with one; a failed send is
reported and never raises."""

from __future__ import annotations

from datetime import datetime, timezone

from hcsc.datalake.dre.config.models import EmailServer
from hcsc.datalake.dre.notify import email
from hcsc.datalake.dre.notify.digest import Digest


def digest() -> Digest:
    return Digest("membership-gold", ["dre-alerts@example.com"], "dev", "run-1",
                  datetime(2026, 1, 16, 12, 5, tzinfo=timezone.utc), passing=3)


class FakeSMTP:
    sent: list = []

    def __init__(self, host: str, port: int, timeout: int) -> None:
        self.host, self.port = host, port

    def __enter__(self) -> FakeSMTP:
        return self

    def __exit__(self, *exc) -> None:
        pass

    def send_message(self, msg) -> None:
        FakeSMTP.sent.append((self.host, self.port, msg))


def test_not_sent_without_a_relay() -> None:
    assert email.send([digest()], EmailServer()) == [
        "email for membership-gold: DRE dev: all clear (not sent: email.smtp_host and email.sender are not set)"]


def test_sent_through_smtp(monkeypatch) -> None:
    monkeypatch.setattr(email.smtplib, "SMTP", FakeSMTP)
    FakeSMTP.sent = []
    server = EmailServer(smtp_host="relay.example.com", smtp_port=2525, sender="dre@example.com")
    assert email.send([digest()], server) == ["email for membership-gold: DRE dev: all clear (sent to 1)"]
    [(host, port, msg)] = FakeSMTP.sent
    assert (host, port, msg["Subject"], msg["To"], msg["From"]) == (
        "relay.example.com", 2525, "DRE dev: all clear", "dre-alerts@example.com", "dre@example.com")
    assert msg.get_content().startswith("DRE dev: all clear\n")


def test_a_failed_send_is_reported(monkeypatch) -> None:
    class Down(FakeSMTP):
        def __enter__(self):
            raise ConnectionRefusedError("synthetic")

    monkeypatch.setattr(email.smtplib, "SMTP", Down)
    server = EmailServer(smtp_host="relay.example.com", sender="dre@example.com")
    assert email.send([digest()], server) == ["email for membership-gold: not sent (ConnectionRefusedError)"]
