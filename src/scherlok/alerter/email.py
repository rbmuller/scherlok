"""Email alerter — sends anomalies via SMTP.

Configuration via environment variables:
    SCHERLOK_SMTP_HOST       — required (e.g. smtp.gmail.com)
    SCHERLOK_SMTP_PORT       — default 587
    SCHERLOK_SMTP_USER       — required (sender login)
    SCHERLOK_SMTP_PASSWORD   — required (or app password)
    SCHERLOK_SMTP_FROM       — default = SMTP_USER
    SCHERLOK_SMTP_USE_TLS    — default true
"""

import html
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from scherlok.detector.severity import Severity
from scherlok.explainer import format_explanation_text, format_unavailable_note

logger = logging.getLogger(__name__)

SEVERITY_COLOR = {
    Severity.INFO: "#3b82f6",
    Severity.WARNING: "#f59e0b",
    Severity.CRITICAL: "#ef4444",
}


def _explanation_html(explanation: dict | None, explain_note: str | None) -> str:
    """HTML block for the --explain hypothesis (or its fallback note).

    Model output is untrusted text landing in an HTML document — escape it.
    """
    if explanation:
        summary = html.escape(str(explanation.get("summary", "")))
        cause = html.escape(str(explanation.get("likely_cause", "")))
        steps = explanation.get("diagnostic_steps") or []
        steps_html = "".join(f"<li>{html.escape(str(s))}</li>" for s in steps)
        steps_block = (
            f'<p style="margin:8px 0 0"><strong>Check next:</strong></p>'
            f'<ol style="margin:4px 0 0">{steps_html}</ol>'
            if steps
            else ""
        )
        return (
            '<div style="margin-top:16px;padding:12px 16px;'
            'border-left:4px solid #6b46c1;background:#faf5ff">'
            '<p style="margin:0 0 8px"><strong>🧠 AI hypothesis</strong> '
            '<span style="color:#64748b">(--explain)</span></p>'
            f'<p style="margin:0 0 8px">{summary}</p>'
            f'<p style="margin:0"><strong>Likely cause:</strong> {cause}</p>'
            f"{steps_block}"
            "</div>"
        )
    if explain_note:
        note = html.escape(format_unavailable_note(explain_note))
        return f'<p style="color:#64748b;font-size:12px;margin-top:16px">{note}</p>'
    return ""


def _build_html(
    anomalies: list[dict],
    explanation: dict | None = None,
    explain_note: str | None = None,
) -> str:
    """Build HTML email body."""
    cell = "padding:8px;border-bottom:1px solid #e5e7eb"
    rows = []
    for a in anomalies:
        color = SEVERITY_COLOR.get(a["severity"], "#64748b")
        rows.append(
            f"<tr>"
            f'<td style="{cell};color:{color};font-weight:bold">{a["severity"].value}</td>'
            f'<td style="{cell};font-family:monospace">{a["table"]}</td>'
            f'<td style="{cell}">{a["type"]}</td>'
            f'<td style="{cell}">{a["message"]}</td>'
            f"</tr>"
        )

    body_style = (
        "font-family:-apple-system,BlinkMacSystemFont,sans-serif;"
        "color:#111;max-width:720px;margin:0 auto;padding:24px"
    )
    return f"""\
<!DOCTYPE html>
<html>
<body style="{body_style}">
<h2 style="color:#0d9488">🔍 Scherlok Data Quality Alert</h2>
<p>Detected <strong>{len(anomalies)}</strong> anomalies in your data.</p>
<table style="width:100%;border-collapse:collapse;margin-top:16px">
<thead>
<tr style="background:#f3f4f6">
<th style="padding:8px;text-align:left">Severity</th>
<th style="padding:8px;text-align:left">Table</th>
<th style="padding:8px;text-align:left">Type</th>
<th style="padding:8px;text-align:left">Message</th>
</tr>
</thead>
<tbody>
{"".join(rows)}
</tbody>
</table>
{_explanation_html(explanation, explain_note)}
<p style="color:#64748b;font-size:12px;margin-top:24px">
Sent by <a href="https://github.com/rbmuller/scherlok" style="color:#0d9488">Scherlok</a>
</p>
</body>
</html>
"""


def _build_text(
    anomalies: list[dict],
    explanation: dict | None = None,
    explain_note: str | None = None,
) -> str:
    """Plain text fallback."""
    lines = [f"Scherlok detected {len(anomalies)} anomalies:", ""]
    for a in anomalies:
        lines.append(f"[{a['severity'].value}] {a['table']} — {a['type']}: {a['message']}")
    if explanation:
        lines.extend(["", format_explanation_text(explanation)])
    elif explain_note:
        lines.extend(["", format_unavailable_note(explain_note)])
    return "\n".join(lines)


def send_email_alert(
    to_addresses: list[str],
    anomalies: list[dict],
    explanation: dict | None = None,
    explain_note: str | None = None,
) -> bool:
    """Send email alert with anomalies.

    `explanation` (the --explain hypothesis) is rendered as a highlighted
    block under the anomaly table; `explain_note` is the fallback one-liner
    when the explanation could not be produced.

    Returns True on success, False if SMTP misconfigured or send fails.
    """
    if not anomalies:
        return True

    host = os.environ.get("SCHERLOK_SMTP_HOST")
    user = os.environ.get("SCHERLOK_SMTP_USER")
    password = os.environ.get("SCHERLOK_SMTP_PASSWORD")
    if not host or not user or not password:
        logger.warning(
            "SCHERLOK_SMTP_HOST/USER/PASSWORD env vars required for email alerts"
        )
        return False

    port = int(os.environ.get("SCHERLOK_SMTP_PORT", "587"))
    sender = os.environ.get("SCHERLOK_SMTP_FROM", user)
    use_tls = os.environ.get("SCHERLOK_SMTP_USE_TLS", "true").lower() == "true"

    msg = MIMEMultipart("alternative")
    critical_count = sum(1 for a in anomalies if a["severity"] == Severity.CRITICAL)
    prefix = "🔴 CRITICAL · " if critical_count else "⚠️  "
    msg["Subject"] = f"{prefix}Scherlok: {len(anomalies)} anomalies detected"
    msg["From"] = sender
    msg["To"] = ", ".join(to_addresses)

    msg.attach(MIMEText(_build_text(anomalies, explanation, explain_note), "plain"))
    msg.attach(MIMEText(_build_html(anomalies, explanation, explain_note), "html"))

    try:
        with smtplib.SMTP(host, port, timeout=15) as smtp:
            if use_tls:
                smtp.starttls()
            smtp.login(user, password)
            smtp.send_message(msg)
        logger.info("Email sent to %d recipients", len(to_addresses))
        return True
    except Exception as e:
        logger.warning("Email delivery failed: %s", e)
        return False
