"""Tests for alerter modules (Slack, exit codes, console)."""

from unittest.mock import MagicMock, patch

import pytest

from scherlok.alerter.exitcode import exit_code_for, fails_gate, normalize_fail_on
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


def _anomaly(severity):
    return {"table": "t", "type": "x", "severity": severity, "message": "m"}


class TestFailOn:
    def test_critical_fails_only_on_critical(self):
        assert fails_gate([_anomaly(Severity.CRITICAL)], "critical") is True
        assert fails_gate([_anomaly(Severity.WARNING)], "critical") is False
        assert fails_gate([_anomaly(Severity.INFO)], "critical") is False

    def test_warning_fails_on_warning_or_worse(self):
        assert fails_gate([_anomaly(Severity.WARNING)], "warning") is True
        assert fails_gate([_anomaly(Severity.CRITICAL)], "warning") is True
        assert fails_gate([_anomaly(Severity.INFO)], "warning") is False

    def test_nothing_fails_an_empty_run(self):
        assert fails_gate([], "warning") is False

    def test_default_threshold_is_critical(self):
        assert fails_gate([_anomaly(Severity.WARNING)]) is False
        assert fails_gate([_anomaly(Severity.CRITICAL)]) is True

    def test_stored_string_severities_count_too(self):
        """Rows read back from the anomalies table carry plain strings."""
        assert fails_gate([_anomaly("CRITICAL")], "critical") is True
        assert fails_gate([_anomaly("WARNING")], "warning") is True

    def test_threshold_is_case_insensitive(self):
        assert normalize_fail_on("WARNING") == "warning"
        assert fails_gate([_anomaly(Severity.WARNING)], "Warning") is True

    def test_unknown_threshold_is_rejected_not_treated_as_critical(self):
        with pytest.raises(ValueError, match="'critical' or 'warning'"):
            normalize_fail_on("warnings")
        with pytest.raises(ValueError):
            fails_gate([_anomaly(Severity.WARNING)], "warnings")

    def test_exit_code_for_matches_the_critical_gate(self):
        for severity in Severity:
            anomalies = [_anomaly(severity)]
            assert exit_code_for(anomalies) == int(fails_gate(anomalies, "critical"))


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
