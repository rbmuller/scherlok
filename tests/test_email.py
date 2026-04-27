"""Tests for email alerter (SMTP)."""

from unittest.mock import MagicMock, patch

from scherlok.alerter.email import (
    _build_html,
    _build_text,
    send_email_alert,
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


class TestBuilders:
    def test_html_contains_anomalies(self):
        html = _build_html(_anomalies())
        assert "Scherlok Data Quality Alert" in html
        assert "orders" in html
        assert "Row count dropped 52%" in html
        assert "CRITICAL" in html

    def test_text_contains_anomalies(self):
        text = _build_text(_anomalies())
        assert "2 anomalies" in text
        assert "orders" in text
        assert "users" in text


class TestSendEmail:
    def test_empty_anomalies_returns_true(self):
        assert send_email_alert(["a@b.com"], []) is True

    @patch.dict("os.environ", {}, clear=True)
    def test_missing_smtp_config_returns_false(self):
        assert send_email_alert(["a@b.com"], _anomalies()) is False

    @patch.dict("os.environ", {
        "SCHERLOK_SMTP_HOST": "smtp.example.com",
        "SCHERLOK_SMTP_USER": "user@example.com",
        "SCHERLOK_SMTP_PASSWORD": "pass",
    })
    @patch("scherlok.alerter.email.smtplib.SMTP")
    def test_sends_successfully(self, mock_smtp_class):
        mock_smtp = MagicMock()
        mock_smtp_class.return_value.__enter__.return_value = mock_smtp

        result = send_email_alert(["recipient@example.com"], _anomalies())
        assert result is True
        mock_smtp.starttls.assert_called_once()
        mock_smtp.login.assert_called_once_with("user@example.com", "pass")
        mock_smtp.send_message.assert_called_once()

    @patch.dict("os.environ", {
        "SCHERLOK_SMTP_HOST": "smtp.example.com",
        "SCHERLOK_SMTP_USER": "user@example.com",
        "SCHERLOK_SMTP_PASSWORD": "pass",
    })
    @patch("scherlok.alerter.email.smtplib.SMTP")
    def test_returns_false_on_smtp_error(self, mock_smtp_class):
        mock_smtp_class.side_effect = ConnectionRefusedError("nope")
        assert send_email_alert(["a@b.com"], _anomalies()) is False

    @patch.dict("os.environ", {
        "SCHERLOK_SMTP_HOST": "smtp.example.com",
        "SCHERLOK_SMTP_USER": "user@example.com",
        "SCHERLOK_SMTP_PASSWORD": "pass",
        "SCHERLOK_SMTP_USE_TLS": "false",
    })
    @patch("scherlok.alerter.email.smtplib.SMTP")
    def test_skips_tls_when_disabled(self, mock_smtp_class):
        mock_smtp = MagicMock()
        mock_smtp_class.return_value.__enter__.return_value = mock_smtp

        send_email_alert(["a@b.com"], _anomalies())
        mock_smtp.starttls.assert_not_called()
