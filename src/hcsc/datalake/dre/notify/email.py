"""Send digests by SMTP (spec section 8). Standard library only: no runtime service of our own.

With email.smtp_host unset the digests are built but not sent, and the caller is told so.
Nothing is logged but the owner, the recipient count and the subject (counts and codes only).
"""

from __future__ import annotations

import logging
import smtplib
from email.message import EmailMessage
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hcsc.datalake.dre.config.models import EmailServer
    from hcsc.datalake.dre.notify.digest import Digest

log = logging.getLogger(__name__)


def message(subject: str, body: str, recipients: list[str], sender: str) -> EmailMessage:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)
    return msg


def send_text(owner: str, subject: str, body: str, recipients: list[str], server: EmailServer) -> str:
    """Send one plain-text email; return a status line. Never raises: a failed send is reported."""
    if server.smtp_host is None or server.sender is None:
        return f"email for {owner}: {subject} (not sent: email.smtp_host and email.sender are not set)"
    if not recipients:
        return f"email for {owner}: not sent (no recipients)"
    try:
        with smtplib.SMTP(server.smtp_host, server.smtp_port, timeout=30) as smtp:
            smtp.send_message(message(subject, body, recipients, server.sender))
    except (OSError, smtplib.SMTPException) as exc:
        log.error("could not send the email for %s: %s", owner, type(exc).__name__)
        return f"email for {owner}: not sent ({type(exc).__name__})"
    return f"email for {owner}: {subject} (sent to {len(recipients)})"


def send(digests: list[Digest], server: EmailServer) -> list[str]:
    """Send each digest; return one status line per digest."""
    return [send_text(d.owner, d.subject, d.text(), d.recipients, server) for d in digests]
