"""Slack webhook notifications for Scherlok anomalies."""

import requests

from scherlok.detector.severity import Severity

SEVERITY_EMOJI: dict[Severity, str] = {
    Severity.INFO: ":information_source:",
    Severity.WARNING: ":warning:",
    Severity.CRITICAL: ":rotating_light:",
}


def send_slack_alert(webhook_url: str, anomalies: list[dict]) -> bool:
    """Send a formatted Slack message with detected anomalies.

    Returns True if the message was sent successfully.
    """
    if not anomalies:
        return True

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "Scherlok Data Quality Alert",
            },
        },
    ]

    for anomaly in anomalies:
        severity = anomaly["severity"]
        emoji = SEVERITY_EMOJI.get(severity, "")
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{emoji} *[{severity.value}]* `{anomaly['table']}`\n"
                    f"{anomaly['message']}"
                ),
            },
        })

    payload = {"blocks": blocks}

    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        return resp.status_code == 200
    except requests.RequestException:
        return False
