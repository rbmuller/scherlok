"""Tests for alerter modules (Slack, exit codes, console)."""

from unittest.mock import MagicMock, patch

from scherlok.alerter.exitcode import exit_code_for
from scherlok.alerter.slack import send_slack_alert
from scherlok.detector.severity import Severity


class TestExitCode:
    def test_returns_1_on_critical(self):
        anomalies = [{
            "table": "users", "type": "volume_drop",
            "severity": Severity.CRITICAL, "message": "dropped 60%",
        }]
        assert exit_code_for(anomalies) == 1

    def test_returns_0_on_warning_only(self):
        anomalies = [{
            "table": "users", "type": "volume_drop",
            "severity": Severity.WARNING, "message": "dropped 25%",
        }]
        assert exit_code_for(anomalies) == 0

    def test_returns_0_on_empty(self):
        assert exit_code_for([]) == 0

    def test_returns_1_if_any_critical(self):
        anomalies = [
            {"table": "users", "type": "x", "severity": Severity.INFO, "message": "ok"},
            {"table": "orders", "type": "x", "severity": Severity.CRITICAL, "message": "bad"},
            {"table": "products", "type": "x", "severity": Severity.WARNING, "message": "meh"},
        ]
        assert exit_code_for(anomalies) == 1

    def test_returns_0_on_info_only(self):
        anomalies = [
            {"table": "users", "type": "x", "severity": Severity.INFO, "message": "ok"},
        ]
        assert exit_code_for(anomalies) == 0


class TestSlackAlert:
    @patch("scherlok.alerter.slack.requests.post")
    def test_sends_slack_message(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200)
        anomalies = [{
            "table": "users", "type": "volume_drop",
            "severity": Severity.CRITICAL, "message": "dropped 60%",
        }]
        result = send_slack_alert("https://hooks.slack.com/xxx", anomalies)
        assert result is True
        mock_post.assert_called_once()
        payload = mock_post.call_args[1]["json"]
        assert payload["blocks"][0]["text"]["text"] == "Scherlok Data Quality Alert"

    @patch("scherlok.alerter.slack.requests.post")
    def test_returns_false_on_failure(self, mock_post):
        mock_post.return_value = MagicMock(status_code=500)
        anomalies = [
            {"table": "users", "type": "x", "severity": Severity.WARNING, "message": "test"},
        ]
        result = send_slack_alert("https://hooks.slack.com/xxx", anomalies)
        assert result is False

    @patch("scherlok.alerter.slack.requests.post")
    def test_returns_false_on_exception(self, mock_post):
        import requests
        mock_post.side_effect = requests.RequestException("timeout")
        result = send_slack_alert("https://hooks.slack.com/xxx", [
            {"table": "t", "type": "x", "severity": Severity.INFO, "message": "m"},
        ])
        assert result is False

    def test_empty_anomalies_returns_true(self):
        result = send_slack_alert("https://hooks.slack.com/xxx", [])
        assert result is True
