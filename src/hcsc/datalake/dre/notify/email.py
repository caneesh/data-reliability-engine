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


def message(digest: Digest, sender: str) -> EmailMessage:
    msg = EmailMessage()
    msg["Subject"] = digest.subject
    msg["From"] = sender
    msg["To"] = ", ".join(digest.recipients)
    msg.set_content(digest.text())
    return msg


def send(digests: list[Digest], server: EmailServer) -> list[str]:
    """Send each digest; return one status line per digest. Never raises: a failed send is reported."""
    lines = []
    if server.smtp_host is None or server.sender is None:
        return [f"email for {d.owner}: {d.subject} (not sent: email.smtp_host and email.sender are not set)"
                for d in digests]
    for digest in digests:
        if not digest.recipients:
            lines.append(f"email for {digest.owner}: not sent (no recipients)")
            continue
        try:
            with smtplib.SMTP(server.smtp_host, server.smtp_port, timeout=30) as smtp:
                smtp.send_message(message(digest, server.sender))
            lines.append(f"email for {digest.owner}: {digest.subject} (sent to {len(digest.recipients)})")
        except (OSError, smtplib.SMTPException) as exc:
            log.error("could not send the digest for %s: %s", digest.owner, type(exc).__name__)
            lines.append(f"email for {digest.owner}: not sent ({type(exc).__name__})")
    return lines
