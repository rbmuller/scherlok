"""Generic webhook alerter — sends anomalies to any HTTP endpoint.

Covers Slack, Teams, Discord, PagerDuty, Opsgenie, and any custom URL.
Auto-detects the platform from the URL and formats the payload accordingly.
Falls back to a generic JSON payload for unknown endpoints.
"""

import html
import logging

import requests

from scherlok.detector.severity import Severity
from scherlok.explainer import (
    EXPLANATION_COLOR,
    format_explanation_text,
    format_unavailable_note,
)

logger = logging.getLogger(__name__)

PLATFORM_SLACK = "slack"
PLATFORM_DISCORD = "discord"
PLATFORM_TEAMS = "teams"
PLATFORM_GENERIC = "generic"

# Hard payload limits enforced by the platforms; exceeding them rejects the
# WHOLE message, so injected explanation text must never push past them.
SLACK_SECTION_TEXT_LIMIT = 3000
DISCORD_CONTENT_LIMIT = 2000
TEAMS_TEXT_LIMIT = 3000
# Below this leftover budget, skip the explanation rather than ship a stub.
MIN_APPEND_BUDGET = 80

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


def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def _escape_mrkdwn(text: str) -> str:
    """Slack mrkdwn requires &, <, > as entities; '<!channel>'-style sequences
    in untrusted text would otherwise trigger real broadcasts."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _slack_explanation_attachment(explanation: dict) -> dict:
    """Slack attachment carrying the --explain hypothesis under the alert."""
    text = _truncate(
        _escape_mrkdwn(format_explanation_text(explanation)),
        SLACK_SECTION_TEXT_LIMIT,
    )
    return {
        "color": EXPLANATION_COLOR,
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": text}},
        ],
    }


def _teams_explanation_html(explanation: dict) -> str:
    """MessageCard text renders markup — escape before adding <br> tags."""
    escaped = html.escape(format_explanation_text(explanation))
    return _truncate(escaped, TEAMS_TEXT_LIMIT).replace("\n", "<br>")


def _append_within_limit(base: str, suffix: str, limit: int) -> str:
    """Append suffix only if a useful amount fits under the platform limit.

    The original alert always wins: when the leftover budget is too small,
    the suffix is dropped entirely rather than risking a rejected payload.
    """
    budget = limit - len(base)
    if budget < MIN_APPEND_BUDGET:
        return base
    return base + _truncate(suffix, budget)


def _inject_explanation(
    payload: dict,
    platform: str,
    explanation: dict | None,
    explain_note: str | None,
) -> None:
    """Mutate the platform payload in place with the hypothesis or the
    unavailable-note. No-op when neither is set (the `--explain`-off path)."""
    if explanation:
        if platform == PLATFORM_SLACK:
            payload["attachments"] = [_slack_explanation_attachment(explanation)]
        elif platform == PLATFORM_DISCORD:
            payload["content"] = _append_within_limit(
                payload["content"],
                "\n\n" + format_explanation_text(explanation),
                DISCORD_CONTENT_LIMIT,
            )
        elif platform == PLATFORM_TEAMS:
            payload["text"] += "<br><br>" + _teams_explanation_html(explanation)
        else:
            payload["explanation"] = explanation
    elif explain_note:
        note = format_unavailable_note(explain_note)
        if platform == PLATFORM_SLACK:
            payload["blocks"].append(
                {
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": _escape_mrkdwn(note)}],
                }
            )
        elif platform == PLATFORM_DISCORD:
            payload["content"] = _append_within_limit(
                payload["content"], f"\n\n{note}", DISCORD_CONTENT_LIMIT
            )
        elif platform == PLATFORM_TEAMS:
            payload["text"] += f"<br><br>{html.escape(note)}"
        else:
            payload["explanation_error"] = explain_note


def _detect_platform(url: str) -> str:
    """Auto-detect platform from webhook URL."""
    url_lower = url.lower()
    if "hooks.slack.com" in url_lower or "slack" in url_lower:
        return PLATFORM_SLACK
    if "discord.com/api/webhooks" in url_lower or "discordapp.com" in url_lower:
        return PLATFORM_DISCORD
    if "office.com" in url_lower or "teams" in url_lower:
        return PLATFORM_TEAMS
    return PLATFORM_GENERIC


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
        PLATFORM_SLACK: _payload_slack,
        PLATFORM_DISCORD: _payload_discord,
        PLATFORM_TEAMS: _payload_teams,
        PLATFORM_GENERIC: _payload_generic,
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
