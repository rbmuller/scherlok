"""Tests for the --explain feature: explainer engine, alerter injection,
and CLI dispatch wiring. The Anthropic client is always mocked — no real
API calls happen in CI."""

import json
import re
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from scherlok.cli import _dispatch_alerts, _explain_or_note, _upstream_preview, app
from scherlok.detector.severity import Severity
from scherlok.explainer import (
    ExplainUnavailableError,
    build_bundle,
    engine,
    explain_anomalies,
    format_explanation_text,
    format_unavailable_note,
)
from scherlok.store.sqlite import ProfileStore

runner = CliRunner(env={"NO_COLOR": "1"})
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

EXPLANATION = {
    "summary": "Both anomalies trace back to raw_orders.",
    "likely_cause": "Upstream schema change in raw_orders.",
    "diagnostic_steps": ["Check raw_orders DDL diff", "Run scherlok investigate"],
}


def _anomalies():
    return [
        {
            "table": "orders", "type": "volume_drop",
            "severity": Severity.CRITICAL, "message": "Row count dropped 52%",
        },
        {
            "table": "users", "type": "null_rate_change",
            "severity": Severity.WARNING, "message": "NULL rate 2% → 18%",
        },
    ]


class _FakeBlock:
    def __init__(self, type_, input_=None):
        self.type = type_
        self.input = input_


class _FakeResponse:
    def __init__(self, blocks):
        self.content = blocks


def _fake_client(tool_input=None, error=None, blocks=None):
    """A stand-in for anthropic.Anthropic with a recording messages.create."""
    client = MagicMock()
    if error is not None:
        client.messages.create.side_effect = error
    else:
        if blocks is None:
            blocks = [_FakeBlock("tool_use", tool_input or dict(EXPLANATION))]
        client.messages.create.return_value = _FakeResponse(blocks)
    return client


@pytest.fixture
def fake_client(monkeypatch):
    client = _fake_client()
    monkeypatch.setattr(engine, "_build_client", lambda: client)
    return client


# ---------------------------------------------------------------- bundle


class TestBuildBundle:
    def test_anomalies_are_aggregates_only(self):
        """Extra keys on anomaly dicts must never leak into the bundle."""
        anomalies = [{**_anomalies()[0], "raw_rows": [{"secret": 1}]}]
        bundle = build_bundle(anomalies)
        assert set(bundle["anomalies"][0]) == {"table", "type", "severity", "message"}

    def test_severity_serializes_as_bare_name(self):
        bundle = build_bundle(_anomalies())
        assert bundle["anomalies"][0]["severity"] == "CRITICAL"

    def test_string_severity_passes_through(self):
        bundle = build_bundle(
            [], recent_history=[{"table": "t", "type": "x", "severity": "WARNING"}]
        )
        assert bundle["recent_history"][0]["severity"] == "WARNING"

    def test_history_capped(self):
        history = [
            {"table": "t", "type": "x", "severity": "INFO", "detected_at": str(i)}
            for i in range(50)
        ]
        bundle = build_bundle(_anomalies(), recent_history=history)
        assert len(bundle["recent_history"]) == engine.MAX_HISTORY_ENTRIES

    def test_lineage_capped_and_empty_parents_dropped(self):
        lineage = {"orders": [f"p{i}" for i in range(30)], "users": []}
        bundle = build_bundle(_anomalies(), lineage=lineage)
        assert len(bundle["lineage"]["orders"]) == engine.MAX_LINEAGE_PARENTS
        assert "users" not in bundle["lineage"]

    def test_optional_sections_omitted_when_absent(self):
        bundle = build_bundle(_anomalies())
        assert "lineage" not in bundle
        assert "recent_history" not in bundle
        assert "meta" not in bundle

    def test_meta_passthrough(self):
        bundle = build_bundle(_anomalies(), meta={"adapter": "postgres"})
        assert bundle["meta"] == {"adapter": "postgres"}


# ------------------------------------------------------- explain_anomalies


class TestExplainAnomalies:
    def test_no_anomalies_is_a_noop(self, monkeypatch):
        """Empty bundle: return None without ever building a client."""
        def _boom():
            raise AssertionError("client must not be built")
        monkeypatch.setattr(engine, "_build_client", _boom)
        assert explain_anomalies({"anomalies": []}) is None
        assert explain_anomalies({}) is None

    def test_one_call_with_forced_tool_and_cached_system(self, fake_client):
        bundle = build_bundle(_anomalies())
        result = explain_anomalies(bundle)

        assert result == EXPLANATION
        fake_client.messages.create.assert_called_once()
        kwargs = fake_client.messages.create.call_args.kwargs
        assert kwargs["model"] == engine.DEFAULT_MODEL
        assert kwargs["tool_choice"] == {
            "type": "tool", "name": engine.EXPLANATION_TOOL_NAME,
        }
        assert kwargs["system"][0]["cache_control"] == {"type": "ephemeral"}
        assert json.dumps(bundle, sort_keys=True) in kwargs["messages"][0]["content"]

    def test_model_env_override(self, fake_client, monkeypatch):
        monkeypatch.setenv(engine.MODEL_ENV_VAR, "claude-sonnet-5")
        explain_anomalies(build_bundle(_anomalies()))
        assert fake_client.messages.create.call_args.kwargs["model"] == "claude-sonnet-5"

    def test_api_error_raises_unavailable(self, monkeypatch):
        client = _fake_client(error=RuntimeError("rate limited"))
        monkeypatch.setattr(engine, "_build_client", lambda: client)
        with pytest.raises(ExplainUnavailableError, match="RuntimeError: rate limited"):
            explain_anomalies(build_bundle(_anomalies()))

    def test_missing_tool_use_raises_unavailable(self, monkeypatch):
        client = _fake_client(blocks=[_FakeBlock("text")])
        monkeypatch.setattr(engine, "_build_client", lambda: client)
        with pytest.raises(ExplainUnavailableError, match="no structured explanation"):
            explain_anomalies(build_bundle(_anomalies()))

    def test_incomplete_payload_raises_unavailable(self, monkeypatch):
        client = _fake_client(tool_input={"summary": "", "likely_cause": "x"})
        monkeypatch.setattr(engine, "_build_client", lambda: client)
        with pytest.raises(ExplainUnavailableError, match="incomplete"):
            explain_anomalies(build_bundle(_anomalies()))

    def test_diagnostic_steps_truncated(self, monkeypatch):
        tool_input = {**EXPLANATION, "diagnostic_steps": ["a", "b", "c", "d", "e"]}
        client = _fake_client(tool_input=tool_input)
        monkeypatch.setattr(engine, "_build_client", lambda: client)
        result = explain_anomalies(build_bundle(_anomalies()))
        assert result["diagnostic_steps"] == ["a", "b", "c"]

    def test_missing_api_key_raises_unavailable(self, monkeypatch):
        monkeypatch.delenv(engine.API_KEY_ENV_VAR, raising=False)
        monkeypatch.delenv(engine.AUTH_TOKEN_ENV_VAR, raising=False)
        with pytest.raises(ExplainUnavailableError, match=engine.API_KEY_ENV_VAR):
            explain_anomalies(build_bundle(_anomalies()))

    def test_build_client_constructs_real_client(self, monkeypatch):
        """Catches constructor-arg drift (timeout/max_retries) against the
        installed anthropic SDK. No request is made."""
        monkeypatch.setenv(engine.API_KEY_ENV_VAR, "test-key")
        client = engine._build_client()
        assert type(client).__name__ == "Anthropic"


# ------------------------------------------------------------- formatting


class TestFormatting:
    def test_text_rendering_has_all_parts(self):
        text = format_explanation_text(EXPLANATION)
        assert engine.EXPLANATION_HEADER in text
        assert "Likely cause: Upstream schema change in raw_orders." in text
        assert "1. Check raw_orders DDL diff" in text

    def test_unavailable_note(self):
        assert format_unavailable_note("boom") == "(--explain unavailable: boom)"


# ------------------------------------------------------ webhook injection


class TestWebhookInjection:
    def _payload_for(self, url, explanation=None, explain_note=None):
        from scherlok.alerter.webhook import send_webhook

        with patch("scherlok.alerter.webhook.requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            send_webhook(url, _anomalies(), explanation=explanation, explain_note=explain_note)
            return mock_post.call_args[1]["json"]

    def test_slack_gets_attachment(self):
        payload = self._payload_for("https://hooks.slack.com/x", explanation=EXPLANATION)
        attachment = payload["attachments"][0]
        text = attachment["blocks"][0]["text"]["text"]
        assert "AI hypothesis" in text
        assert EXPLANATION["likely_cause"] in text
        assert "1. Check raw_orders DDL diff" in text

    def test_generic_gets_explanation_field(self):
        payload = self._payload_for("https://my-api.com/x", explanation=EXPLANATION)
        assert payload["explanation"] == EXPLANATION

    def test_discord_appends_text(self):
        url = "https://discord.com/api/webhooks/1/a"
        payload = self._payload_for(url, explanation=EXPLANATION)
        assert EXPLANATION["summary"] in payload["content"]

    def test_teams_appends_html_breaks(self):
        url = "https://outlook.office.com/webhook/x"
        payload = self._payload_for(url, explanation=EXPLANATION)
        assert EXPLANATION["summary"] in payload["text"]
        assert "<br>" in payload["text"]

    def test_note_on_failure_generic(self):
        payload = self._payload_for("https://my-api.com/x", explain_note="no key")
        assert payload["explanation_error"] == "no key"
        assert "explanation" not in payload

    def test_note_on_failure_slack(self):
        payload = self._payload_for("https://hooks.slack.com/x", explain_note="no key")
        context = payload["blocks"][-1]
        assert context["type"] == "context"
        assert "(--explain unavailable: no key)" in context["elements"][0]["text"]

    def test_payload_untouched_without_explain(self):
        payload = self._payload_for("https://my-api.com/x")
        assert "explanation" not in payload
        assert "explanation_error" not in payload


# -------------------------------------------------------- email injection


class TestEmailInjection:
    def test_html_includes_escaped_hypothesis(self):
        from scherlok.alerter.email import _build_html

        explanation = {**EXPLANATION, "summary": "<script>alert(1)</script>"}
        html_body = _build_html(_anomalies(), explanation)
        assert "AI hypothesis" in html_body
        assert "&lt;script&gt;" in html_body
        assert "<script>" not in html_body

    def test_text_includes_hypothesis(self):
        from scherlok.alerter.email import _build_text

        text = _build_text(_anomalies(), EXPLANATION)
        assert EXPLANATION["likely_cause"] in text

    def test_text_includes_note_on_failure(self):
        from scherlok.alerter.email import _build_text

        text = _build_text(_anomalies(), None, "no key")
        assert "(--explain unavailable: no key)" in text

    def test_bodies_untouched_without_explain(self):
        from scherlok.alerter.email import _build_html, _build_text

        assert "AI hypothesis" not in _build_html(_anomalies())
        assert "--explain" not in _build_text(_anomalies())


# -------------------------------------------------------- dispatch wiring


class TestDispatchWiring:
    def test_explain_or_note_success_filters_history_to_anomalous_tables(
        self, fake_client, tmp_path
    ):
        store = ProfileStore(db_path=tmp_path / "profiles.db")
        store.save_anomalies(
            [
                {
                    "table": "orders", "type": "volume_drop",
                    "severity": Severity.WARNING, "message": "old one",
                },
                {
                    "table": "unrelated", "type": "table_empty",
                    "severity": Severity.CRITICAL, "message": "elsewhere",
                },
            ]
        )
        explanation, note = _explain_or_note(_anomalies(), store, None)

        assert explanation == EXPLANATION
        assert note is None
        content = fake_client.messages.create.call_args.kwargs["messages"][0]["content"]
        sent = json.loads(content.split("\n", 1)[1])
        history_tables = {h["table"] for h in sent["recent_history"]}
        assert history_tables == {"orders"}  # "unrelated" filtered out

    def test_explain_or_note_returns_note_on_failure(self, monkeypatch, tmp_path):
        monkeypatch.delenv(engine.API_KEY_ENV_VAR, raising=False)
        monkeypatch.delenv(engine.AUTH_TOKEN_ENV_VAR, raising=False)
        store = ProfileStore(db_path=tmp_path / "profiles.db")
        explanation, note = _explain_or_note(_anomalies(), store, None)
        assert explanation is None
        assert engine.API_KEY_ENV_VAR in note

    def test_dispatch_reads_history_before_saving_current_batch(
        self, fake_client, tmp_path
    ):
        """The bundle must see only prior runs — not the batch being alerted."""
        store = ProfileStore(db_path=tmp_path / "profiles.db")
        _dispatch_alerts(_anomalies(), store, None, None, explain=True)

        content = fake_client.messages.create.call_args.kwargs["messages"][0]["content"]
        assert "recent_history" not in content  # fresh store: nothing prior
        assert len(store.get_anomaly_history(days=1)) == 2  # but batch was saved

    @patch("scherlok.cli.send_webhook")
    def test_dispatch_forwards_explanation_to_webhook(self, mock_webhook, tmp_path):
        mock_webhook.return_value = True
        store = ProfileStore(db_path=tmp_path / "profiles.db")
        with patch(
            "scherlok.cli._explain_or_note", return_value=(dict(EXPLANATION), None)
        ):
            _dispatch_alerts(_anomalies(), store, "https://hooks.slack.com/x", None, explain=True)
        kwargs = mock_webhook.call_args.kwargs
        assert kwargs["explanation"] == EXPLANATION
        assert kwargs["explain_note"] is None

    @patch("scherlok.cli.send_webhook")
    def test_dispatch_fail_open_sends_unaugmented_alert_with_note(
        self, mock_webhook, tmp_path
    ):
        mock_webhook.return_value = True
        store = ProfileStore(db_path=tmp_path / "profiles.db")
        with patch("scherlok.cli._explain_or_note", return_value=(None, "no key")):
            _dispatch_alerts(_anomalies(), store, "https://hooks.slack.com/x", None, explain=True)
        kwargs = mock_webhook.call_args.kwargs
        assert kwargs["explanation"] is None
        assert kwargs["explain_note"] == "no key"
        assert len(store.get_anomaly_history(days=1)) == 2

    @patch("scherlok.cli.send_webhook")
    def test_dispatch_without_explain_never_calls_explainer(self, mock_webhook, tmp_path):
        mock_webhook.return_value = True
        store = ProfileStore(db_path=tmp_path / "profiles.db")
        with patch("scherlok.cli._explain_or_note") as mock_explain:
            _dispatch_alerts(_anomalies(), store, "https://hooks.slack.com/x", None)
        mock_explain.assert_not_called()
        assert mock_webhook.call_args.kwargs["explanation"] is None


# ----------------------------------------------- code-review regressions


class TestFailOpenRegressions:
    """The fail-open contract must hold against malformed inputs, malformed
    API responses, and error text that collides with Rich markup."""

    def test_anomaly_without_table_key_still_explains(self, fake_client, tmp_path):
        store = ProfileStore(db_path=tmp_path / "profiles.db")
        anomalies = [{"type": "x", "severity": Severity.INFO, "message": "m"}]
        explanation, note = _explain_or_note(anomalies, store, None)
        assert explanation == EXPLANATION
        assert note is None

    def test_malformed_api_response_content_becomes_note(self, monkeypatch, tmp_path):
        client = MagicMock()
        client.messages.create.return_value = _FakeResponse(None)  # content=None
        monkeypatch.setattr(engine, "_build_client", lambda: client)
        store = ProfileStore(db_path=tmp_path / "profiles.db")
        explanation, note = _explain_or_note(_anomalies(), store, None)
        assert explanation is None
        assert "TypeError" in note

    def test_unexpected_error_hits_the_safety_net(self, tmp_path):
        store = ProfileStore(db_path=tmp_path / "profiles.db")
        with patch("scherlok.explainer.build_bundle", side_effect=RuntimeError("boom")):
            explanation, note = _explain_or_note(_anomalies(), store, None)
        assert explanation is None
        assert "RuntimeError: boom" in note

    def test_note_with_rich_bracket_text_is_escaped(self, tmp_path):
        store = ProfileStore(db_path=tmp_path / "profiles.db")
        note = "pip install 'scherlok[explain]'"
        with (
            patch("scherlok.cli._explain_or_note", return_value=(None, note)),
            patch("scherlok.cli.out_error") as mock_err,
        ):
            _dispatch_alerts(_anomalies(), store, None, None, explain=True)
        printed = mock_err.call_args_list[0].args[0]
        assert "\\[explain]" in printed  # bracket escaped, not eaten as markup

    def test_note_with_closing_tag_does_not_crash_dispatch(self, tmp_path):
        """A '[/x]' sequence used to raise rich MarkupError mid-dispatch."""
        store = ProfileStore(db_path=tmp_path / "profiles.db")
        with patch("scherlok.cli._explain_or_note", return_value=(None, "body [/x] tag")):
            _dispatch_alerts(_anomalies(), store, None, None, explain=True)
        assert len(store.get_anomaly_history(days=1)) == 2

    def test_steps_as_string_becomes_single_step(self, monkeypatch):
        tool_input = {**EXPLANATION, "diagnostic_steps": "Check dbt logs"}
        client = _fake_client(tool_input=tool_input)
        monkeypatch.setattr(engine, "_build_client", lambda: client)
        result = explain_anomalies(build_bundle(_anomalies()))
        assert result["diagnostic_steps"] == ["Check dbt logs"]

    def test_dispatch_returns_explanation_for_json_sinks(self, fake_client, tmp_path):
        store = ProfileStore(db_path=tmp_path / "profiles.db")
        explanation, note = _dispatch_alerts(
            _anomalies(), store, None, None, explain=True
        )
        assert explanation == EXPLANATION
        assert note is None


class TestPayloadLimitRegressions:
    """Injected explanation text must never push a payload past platform
    limits — an oversized payload rejects the WHOLE alert."""

    def _huge_explanation(self):
        return {
            "summary": "s" * 2000,
            "likely_cause": "c" * 2000,
            "diagnostic_steps": ["x" * 500, "y" * 500, "z" * 500],
        }

    def test_slack_attachment_capped_at_section_limit(self):
        from scherlok.alerter.webhook import (
            SLACK_SECTION_TEXT_LIMIT,
            _slack_explanation_attachment,
        )

        attachment = _slack_explanation_attachment(self._huge_explanation())
        assert len(attachment["blocks"][0]["text"]["text"]) <= SLACK_SECTION_TEXT_LIMIT

    def test_discord_content_never_exceeds_limit(self):
        from scherlok.alerter.webhook import DISCORD_CONTENT_LIMIT, send_webhook

        long_anomalies = [
            {
                "table": f"t{i}", "type": "volume_drop",
                "severity": Severity.CRITICAL, "message": "m" * 120,
            }
            for i in range(14)
        ]
        with patch("scherlok.alerter.webhook.requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            send_webhook(
                "https://discord.com/api/webhooks/1/a",
                long_anomalies,
                explanation=self._huge_explanation(),
            )
            content = mock_post.call_args[1]["json"]["content"]
        assert len(content) <= DISCORD_CONTENT_LIMIT

    def test_discord_drops_explanation_before_truncating_alert(self):
        """When the base alert already fills the budget, the explanation is
        skipped entirely — the alert itself is never cut."""
        from scherlok.alerter.webhook import _append_within_limit

        base = "x" * 1990
        assert _append_within_limit(base, "\n\nhypothesis", 2000) == base


class TestEscapingRegressions:
    def test_slack_escapes_model_output(self):
        from scherlok.alerter.webhook import _slack_explanation_attachment

        attachment = _slack_explanation_attachment(
            {**EXPLANATION, "summary": "count < baseline & <!channel>"}
        )
        text = attachment["blocks"][0]["text"]["text"]
        assert "&lt;!channel&gt;" in text
        assert "count &lt; baseline &amp;" in text

    def test_teams_escapes_model_output(self):
        from scherlok.alerter.webhook import send_webhook

        with patch("scherlok.alerter.webhook.requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            send_webhook(
                "https://outlook.office.com/webhook/x",
                _anomalies(),
                explanation={**EXPLANATION, "summary": "<script>alert(1)</script>"},
            )
            text = mock_post.call_args[1]["json"]["text"]
        assert "&lt;script&gt;" in text
        assert "<script>" not in text


# ------------------------------------------------------------- CLI flags


class TestCliFlags:
    @pytest.mark.parametrize(
        "command",
        [["watch"], ["ci"], ["check"], ["dbt"], ["dbt-run-and-watch"]],
    )
    def test_explain_flag_documented(self, command):
        result = runner.invoke(app, [*command, "--help"])
        assert result.exit_code == 0
        assert "--explain" in ANSI_RE.sub("", result.output)


# --------------------------------------------------------- dbt lineage


class TestUpstreamPreview:
    def test_nearest_ancestors_first_deduped(self):
        graph = {
            "model.p.child": ["model.p.parent_a", "model.p.parent_b"],
            "model.p.parent_a": ["model.p.grandparent"],
            "model.p.parent_b": ["model.p.grandparent"],
            "model.p.grandparent": ["model.p.great_grandparent"],
        }
        names = _upstream_preview(graph, "model.p.child")
        assert names == ["parent_a", "parent_b", "grandparent", "great_grandparent"]

    def test_cycle_does_not_list_model_as_its_own_upstream(self):
        graph = {"model.p.a": ["model.p.b"], "model.p.b": ["model.p.a"]}
        assert _upstream_preview(graph, "model.p.a") == ["b"]

    def test_unknown_node_is_empty(self):
        assert _upstream_preview({}, "model.p.ghost") == []
