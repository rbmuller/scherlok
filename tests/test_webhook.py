"""Tests for the generic webhook alerter."""

from unittest.mock import MagicMock, patch

from scherlok.alerter.webhook import (
    _detect_platform,
    _payload_discord,
    _payload_generic,
    _payload_slack,
    _payload_teams,
    send_webhook,
)
from scherlok.detector.severity import Severity


def _anomalies():
    return [
        {
            "table": "orders", "type": "volume_drop",
            "severity": Severity.CRITICAL, "message": "Row count dropped 52%",
        },
        {
            "table": "users", "type": "null_increase",
            "severity": Severity.WARNING, "message": "NULL rate 2% → 18%",
        },
    ]


class TestDetectPlatform:
    def test_slack(self):
        assert _detect_platform("https://hooks.slack.com/services/T/B/x") == "slack"

    def test_discord(self):
        assert _detect_platform("https://discord.com/api/webhooks/123/abc") == "discord"

    def test_teams(self):
        url = "https://outlook.office.com/webhook/xxx"
        assert _detect_platform(url) == "teams"

    def test_generic(self):
        assert _detect_platform("https://my-api.com/webhook") == "generic"


class TestPayloadFormats:
    def test_slack_has_blocks(self):
        payload = _payload_slack(_anomalies())
        assert "blocks" in payload
        assert payload["blocks"][0]["type"] == "header"

    def test_discord_has_content(self):
        payload = _payload_discord(_anomalies())
        assert "content" in payload
        assert "Scherlok" in payload["content"]

    def test_teams_has_messagcard(self):
        payload = _payload_teams(_anomalies())
        assert payload["@type"] == "MessageCard"
        assert payload["themeColor"] == "FF0000"  # CRITICAL present

    def test_teams_warning_color(self):
        anomalies = [{
            "table": "t", "type": "x",
            "severity": Severity.WARNING, "message": "m",
        }]
        payload = _payload_teams(anomalies)
        assert payload["themeColor"] == "FFA500"

    def test_generic_has_anomalies_list(self):
        payload = _payload_generic(_anomalies())
        assert payload["source"] == "scherlok"
        assert len(payload["anomalies"]) == 2
        assert payload["anomalies"][0]["severity"] == "CRITICAL"


class TestSendWebhook:
    @patch("scherlok.alerter.webhook.requests.post")
    def test_sends_and_returns_true(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200)
        assert send_webhook("https://hooks.slack.com/x", _anomalies()) is True
        mock_post.assert_called_once()

    @patch("scherlok.alerter.webhook.requests.post")
    def test_returns_false_on_error(self, mock_post):
        mock_post.return_value = MagicMock(status_code=500)
        assert send_webhook("https://hooks.slack.com/x", _anomalies()) is False

    @patch("scherlok.alerter.webhook.requests.post")
    def test_returns_false_on_exception(self, mock_post):
        import requests
        mock_post.side_effect = requests.RequestException("timeout")
        assert send_webhook("https://my-api.com/x", _anomalies()) is False

    def test_empty_anomalies_returns_true(self):
        assert send_webhook("https://any-url.com", []) is True

    @patch("scherlok.alerter.webhook.requests.post")
    def test_auto_detects_discord(self, mock_post):
        mock_post.return_value = MagicMock(status_code=204)
        url = "https://discord.com/api/webhooks/123/abc"
        assert send_webhook(url, _anomalies()) is True
        payload = mock_post.call_args[1]["json"]
        assert "content" in payload  # Discord format
