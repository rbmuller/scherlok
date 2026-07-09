"""Generic webhook alerter — sends anomalies to any HTTP endpoint.

Covers Slack, Teams, Discord, PagerDuty, Opsgenie, and any custom URL.
Auto-detects the platform from the URL and formats the payload accordingly.
Falls back to a generic JSON payload for unknown endpoints.
"""

import logging

import requests

from scherlok.detector.severity import Severity
from scherlok.explainer import format_explanation_text, format_unavailable_note

logger = logging.getLogger(__name__)

EXPLANATION_COLOR = "#6B46C1"

SEVERITY_EMOJI = {
    Severity.INFO: "ℹ️",
    Severity.WARNING: "⚠️",
    Severity.CRITICAL: "🔴",
}


def _format_text(anomalies: list[dict]) -> str:
    """Format anomalies as plain text lines."""
    lines = ["Scherlok Data Quality Alert", ""]
    for a in anomalies:
        emoji = SEVERITY_EMOJI.get(a["severity"], "")
        lines.append(f"{emoji} [{a['severity'].value}] {a['table']} — {a['message']}")
    return "\n".join(lines)


def _payload_slack(anomalies: list[dict]) -> dict:
    """Slack Block Kit payload."""
    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": "Scherlok Data Quality Alert"}},
    ]
    for a in anomalies:
        emoji = {
            Severity.INFO: ":information_source:",
            Severity.WARNING: ":warning:",
            Severity.CRITICAL: ":rotating_light:",
        }.get(a["severity"], "")
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{emoji} *[{a['severity'].value}]* `{a['table']}`\n{a['message']}",
            },
        })
    return {"blocks": blocks}


def _payload_discord(anomalies: list[dict]) -> dict:
    """Discord webhook payload."""
    return {"content": _format_text(anomalies)}


def _payload_teams(anomalies: list[dict]) -> dict:
    """Microsoft Teams Incoming Webhook payload."""
    return {
        "@type": "MessageCard",
        "summary": "Scherlok Data Quality Alert",
        "themeColor": "FF0000" if any(
            a["severity"] == Severity.CRITICAL for a in anomalies
        ) else "FFA500",
        "title": "Scherlok Data Quality Alert",
        "text": _format_text(anomalies).replace("\n", "<br>"),
    }


def _payload_generic(anomalies: list[dict]) -> dict:
    """Generic JSON payload — works with any endpoint."""
    return {
        "source": "scherlok",
        "summary": f"{len(anomalies)} anomalies detected",
        "anomalies": [
            {
                "table": a["table"],
                "type": a["type"],
                "severity": a["severity"].value,
                "message": a["message"],
            }
            for a in anomalies
        ],
    }


def _slack_explanation_attachment(explanation: dict) -> dict:
    """Slack attachment carrying the --explain hypothesis under the alert."""
    text = (
        f":brain: *AI hypothesis* (`--explain`)\n"
        f"{explanation.get('summary', '')}\n"
        f"*Likely cause:* {explanation.get('likely_cause', '')}"
    )
    steps = explanation.get("diagnostic_steps") or []
    if steps:
        numbered = "\n".join(f"{i}. {s}" for i, s in enumerate(steps, 1))
        text += f"\n*Check next:*\n{numbered}"
    return {
        "color": EXPLANATION_COLOR,
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": text}},
        ],
    }


def _inject_explanation(
    payload: dict,
    platform: str,
    explanation: dict | None,
    explain_note: str | None,
) -> None:
    """Mutate the platform payload in place with the hypothesis or the
    unavailable-note. No-op when neither is set (the `--explain`-off path)."""
    if explanation:
        if platform == "slack":
            payload["attachments"] = [_slack_explanation_attachment(explanation)]
        elif platform == "discord":
            payload["content"] += "\n\n" + format_explanation_text(explanation)
        elif platform == "teams":
            payload["text"] += "<br><br>" + format_explanation_text(explanation).replace(
                "\n", "<br>"
            )
        else:
            payload["explanation"] = explanation
    elif explain_note:
        note = format_unavailable_note(explain_note)
        if platform == "slack":
            payload["blocks"].append(
                {"type": "context", "elements": [{"type": "mrkdwn", "text": note}]}
            )
        elif platform == "discord":
            payload["content"] += f"\n\n{note}"
        elif platform == "teams":
            payload["text"] += f"<br><br>{note}"
        else:
            payload["explanation_error"] = explain_note


def _detect_platform(url: str) -> str:
    """Auto-detect platform from webhook URL."""
    url_lower = url.lower()
    if "hooks.slack.com" in url_lower or "slack" in url_lower:
        return "slack"
    if "discord.com/api/webhooks" in url_lower or "discordapp.com" in url_lower:
        return "discord"
    if "office.com" in url_lower or "teams" in url_lower:
        return "teams"
    return "generic"


def send_webhook(
    url: str,
    anomalies: list[dict],
    explanation: dict | None = None,
    explain_note: str | None = None,
) -> bool:
    """Send anomalies to a webhook URL. Auto-detects platform format.

    `explanation` (the --explain hypothesis) rides along as a Slack
    attachment, appended text on Discord/Teams, or an `explanation` field
    on the generic JSON payload. `explain_note` is the fallback one-liner
    when the explanation could not be produced.

    Returns True if the request succeeded (2xx status).
    """
    if not anomalies:
        return True

    platform = _detect_platform(url)
    formatters = {
        "slack": _payload_slack,
        "discord": _payload_discord,
        "teams": _payload_teams,
        "generic": _payload_generic,
    }
    payload = formatters[platform](anomalies)
    _inject_explanation(payload, platform, explanation, explain_note)

    try:
        resp = requests.post(url, json=payload, timeout=10)
        ok = 200 <= resp.status_code < 300
        if ok:
            logger.info("Webhook sent to %s (%s)", platform, url[:50])
        else:
            logger.warning("Webhook %s returned %d", url[:50], resp.status_code)
        return ok
    except requests.RequestException as e:
        logger.warning("Webhook failed for %s: %s", url[:50], e)
        return False
