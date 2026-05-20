"""Tests for `scherlok dbt` CLI command."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from scherlok.cli import app

runner = CliRunner()
FIXTURES = Path(__file__).parent / "fixtures" / "dbt"
PG_PROJECT = FIXTURES / "jaffle_shop_postgres"
PROFILES_PG = FIXTURES / "profiles_postgres"


def test_dbt_help():
    result = runner.invoke(app, ["dbt", "--help"])
    assert result.exit_code == 0
    assert "Profile and watch dbt models" in result.output


def test_dbt_missing_manifest():
    """Pointing at a non-dbt directory should fail with a clear message."""
    result = runner.invoke(app, ["dbt", "--project-dir", "/tmp/nonexistent-dbt-project"])
    assert result.exit_code == 1
    assert "manifest.json not found" in result.output


def test_dbt_select_filter_no_match():
    """--select with a name that doesn't exist exits with a clear error."""
    result = runner.invoke(
        app,
        [
            "dbt",
            "--project-dir", str(PG_PROJECT),
            "--connection-string", "postgresql://u:p@host/db",
            "--select", "ghost_model",
        ],
    )
    assert result.exit_code == 1
    assert "No models matched" in result.output


@patch("scherlok.cli.get_connector")
def test_dbt_with_explicit_connection(mock_get_connector, tmp_path, monkeypatch):
    """End-to-end: explicit --connection-string skips profiles.yml entirely."""
    # Force config to live in tmp_path so we don't pollute ~/.scherlok
    monkeypatch.setenv("HOME", str(tmp_path))

    fake = MagicMock()
    fake.connect.return_value = True
    # Match dbt model identifiers exactly
    fake.list_tables.return_value = [
        "stg_customers", "stg_orders", "fct_orders", "dim_customers_inc",
    ]
    mock_get_connector.return_value = fake

    with patch("scherlok.cli._watch_table") as mock_watch:
        mock_watch.return_value = ([], {"row_count": 100})
        result = runner.invoke(
            app,
            [
                "dbt",
                "--project-dir", str(PG_PROJECT),
                "--connection-string", "postgresql://u:p@host/db",
            ],
        )

    assert result.exit_code == 0, result.output
    # 4 materialized models in the postgres fixture
    assert mock_watch.call_count == 4
    assert "Summary:" in result.output


@patch("scherlok.cli.get_connector")
def test_dbt_resolves_from_profiles(mock_get_connector, tmp_path, monkeypatch):
    """When --connection-string is omitted, profiles.yml drives resolution."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("PG_USER", "alice")
    monkeypatch.setenv("PG_PASSWORD", "wonderland")

    fake = MagicMock()
    fake.connect.return_value = True
    fake.list_tables.return_value = [
        "stg_customers", "stg_orders", "fct_orders", "dim_customers_inc",
    ]
    mock_get_connector.return_value = fake

    with patch("scherlok.cli._watch_table") as mock_watch:
        mock_watch.return_value = ([], {"row_count": 50})
        result = runner.invoke(
            app,
            [
                "dbt",
                "--project-dir", str(PG_PROJECT),
                "--profiles-dir", str(PROFILES_PG),
            ],
        )

    assert result.exit_code == 0, result.output
    mock_get_connector.assert_called_with(
        "postgresql://alice:wonderland@localhost:5432/jaffle_db"
    )


@patch("scherlok.cli.get_connector")
def test_dbt_select_filters_models(mock_get_connector, tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))

    fake = MagicMock()
    fake.connect.return_value = True
    fake.list_tables.return_value = ["stg_customers", "stg_orders", "fct_orders"]
    mock_get_connector.return_value = fake

    with patch("scherlok.cli._watch_table") as mock_watch:
        mock_watch.return_value = ([], {"row_count": 1})
        result = runner.invoke(
            app,
            [
                "dbt",
                "--project-dir", str(PG_PROJECT),
                "--connection-string", "postgresql://u:p@h/d",
                "--select", "fct_orders",
            ],
        )

    assert result.exit_code == 0
    assert mock_watch.call_count == 1
    # Exactly fct_orders profiled
    called_table = mock_watch.call_args.args[2]
    assert called_table == "fct_orders"


@patch("scherlok.cli.get_connector")
def test_dbt_output_json_emits_parseable_object(mock_get_connector, tmp_path, monkeypatch):
    """`--output json` emits a single JSON object on stdout with the documented shape."""
    from scherlok.detector.severity import Severity

    monkeypatch.setenv("HOME", str(tmp_path))

    fake = MagicMock()
    fake.connect.return_value = True
    fake.list_tables.return_value = [
        "stg_customers", "stg_orders", "fct_orders", "dim_customers_inc",
    ]
    mock_get_connector.return_value = fake

    sample_anomaly = {
        "table": "stg_customers",
        "severity": Severity.CRITICAL,
        "type": "volume_drop",
        "message": "row count fell 90%",
    }
    with patch("scherlok.cli._watch_table") as mock_watch:
        # First model carries an anomaly so summary counts non-zero criticals.
        mock_watch.side_effect = [
            ([sample_anomaly], {"row_count": 4}),
            ([], {"row_count": 100}),
            ([], {"row_count": 200}),
            ([], {"row_count": 300}),
        ]
        result = runner.invoke(
            app,
            [
                "dbt",
                "--project-dir", str(PG_PROJECT),
                "--connection-string", "postgresql://u:p@host/db",
                "--output", "json",
            ],
        )

    # Default --fail-on=critical exits 1 on the CRITICAL anomaly; the JSON
    # must still have landed on stdout before that exit.
    assert result.stdout.strip().startswith("{"), f"stdout not json: {result.stdout!r}"
    payload = json.loads(result.stdout)
    assert payload["project_dir"] == str(PG_PROJECT)
    assert payload["adapter"] == "postgres"
    assert isinstance(payload["models"], list)
    assert len(payload["models"]) == 4
    first = payload["models"][0]
    assert set(first.keys()) >= {"name", "physical", "resource_type", "row_count", "anomalies"}
    assert first["anomalies"][0]["severity"] == "CRITICAL"
    assert first["anomalies"][0]["type"] == "volume_drop"
    summary = payload["summary"]
    assert set(summary.keys()) >= {"profiled", "anomalies", "critical", "warning"}
    assert summary["profiled"] == 4
    assert summary["anomalies"] == 1
    assert summary["critical"] == 1
    assert summary["warning"] == 0


@patch("scherlok.cli.get_connector")
def test_dbt_output_json_keeps_stdout_clean(mock_get_connector, tmp_path, monkeypatch):
    """No Rich `Investigating ...` or `Summary:` chatter leaks into stdout when --output json."""
    monkeypatch.setenv("HOME", str(tmp_path))

    fake = MagicMock()
    fake.connect.return_value = True
    fake.list_tables.return_value = [
        "stg_customers", "stg_orders", "fct_orders", "dim_customers_inc",
    ]
    mock_get_connector.return_value = fake

    with patch("scherlok.cli._watch_table") as mock_watch:
        mock_watch.return_value = ([], {"row_count": 1})
        result = runner.invoke(
            app,
            [
                "dbt",
                "--project-dir", str(PG_PROJECT),
                "--connection-string", "postgresql://u:p@host/db",
                "--output", "json",
            ],
        )

    assert result.exit_code == 0, result.output
    assert "Investigating" not in result.stdout
    assert "Summary:" not in result.stdout
    payload = json.loads(result.stdout)
    assert payload["summary"]["anomalies"] == 0


def test_dbt_output_invalid_value_rejected(tmp_path, monkeypatch):
    """Unknown --output values exit 1 with a clear message."""
    monkeypatch.setenv("HOME", str(tmp_path))
    result = runner.invoke(
        app,
        [
            "dbt",
            "--project-dir", str(PG_PROJECT),
            "--connection-string", "postgresql://u:p@host/db",
            "--output", "yaml",
        ],
    )
    assert result.exit_code == 1
    assert "Invalid --output" in result.output


# ---------- --show-lineage + per-anomaly downstream enrichment -----------

@patch("scherlok.cli.get_connector")
def test_dbt_show_lineage_prints_trees(mock_get_connector, tmp_path, monkeypatch):
    """--show-lineage prints upstream/downstream ASCII trees under each model."""
    monkeypatch.setenv("HOME", str(tmp_path))

    fake = MagicMock()
    fake.connect.return_value = True
    fake.list_tables.return_value = [
        "stg_customers", "stg_orders", "fct_orders", "dim_customers_inc",
    ]
    mock_get_connector.return_value = fake

    with patch("scherlok.cli._watch_table") as mock_watch:
        mock_watch.return_value = ([], {"row_count": 100})
        result = runner.invoke(
            app,
            [
                "dbt",
                "--project-dir", str(PG_PROJECT),
                "--connection-string", "postgresql://u:p@h/d",
                "--show-lineage",
                "--select", "fct_orders",
            ],
        )

    assert result.exit_code == 0, result.output
    # Upstream section: fct_orders depends on int_orders_pivoted + stg_customers
    assert "Upstream of fct_orders" in result.output
    assert "int_orders_pivoted" in result.output
    assert "stg_customers" in result.output
    # Box-drawing glyphs from the renderer made it through
    assert "├──" in result.output or "└──" in result.output


@patch("scherlok.cli.get_connector")
def test_dbt_show_lineage_off_by_default(mock_get_connector, tmp_path, monkeypatch):
    """Without --show-lineage, no tree blocks are printed."""
    monkeypatch.setenv("HOME", str(tmp_path))

    fake = MagicMock()
    fake.connect.return_value = True
    fake.list_tables.return_value = [
        "stg_customers", "stg_orders", "fct_orders", "dim_customers_inc",
    ]
    mock_get_connector.return_value = fake

    with patch("scherlok.cli._watch_table") as mock_watch:
        mock_watch.return_value = ([], {"row_count": 100})
        result = runner.invoke(
            app,
            [
                "dbt",
                "--project-dir", str(PG_PROJECT),
                "--connection-string", "postgresql://u:p@h/d",
            ],
        )

    assert result.exit_code == 0, result.output
    assert "Upstream of" not in result.output
    assert "Downstream of" not in result.output


@patch("scherlok.cli.get_connector")
def test_dbt_anomaly_message_enriched_with_downstream(mock_get_connector, tmp_path, monkeypatch):
    """When an anomaly fires on a node with descendants, the message names them."""
    from scherlok.detector.severity import Severity

    monkeypatch.setenv("HOME", str(tmp_path))

    fake = MagicMock()
    fake.connect.return_value = True
    fake.list_tables.return_value = [
        "stg_customers", "stg_orders", "fct_orders", "dim_customers_inc",
    ]
    mock_get_connector.return_value = fake

    # stg_customers has fct_orders + dim_customers_inc downstream per the
    # fixture's parent_map. An anomaly on it should mention both.
    anomaly = {
        "table": "stg_customers",
        "severity": Severity.CRITICAL,
        "type": "volume_drop",
        "message": "Row count dropped 60%",
    }
    with patch("scherlok.cli._watch_table") as mock_watch:
        mock_watch.side_effect = [
            ([anomaly], {"row_count": 40}),  # stg_customers
            ([], {"row_count": 100}),
            ([], {"row_count": 200}),
            ([], {"row_count": 300}),
        ]
        result = runner.invoke(
            app,
            [
                "dbt",
                "--project-dir", str(PG_PROJECT),
                "--connection-string", "postgresql://u:p@h/d",
            ],
        )

    # Default --fail-on=critical exits 1 on this CRITICAL
    assert result.exit_code == 1
    # Message must be enriched in place (so alerter payloads carry the same info)
    assert "Affects" in anomaly["message"]
    assert "downstream model" in anomaly["message"]
    assert "fct_orders" in anomaly["message"]


@patch("scherlok.cli.get_connector")
def test_dbt_anomaly_message_unchanged_for_leaf(mock_get_connector, tmp_path, monkeypatch):
    """Anomalies on leaf marts (no descendants) get no `Affects ...` suffix."""
    from scherlok.detector.severity import Severity

    monkeypatch.setenv("HOME", str(tmp_path))

    fake = MagicMock()
    fake.connect.return_value = True
    fake.list_tables.return_value = ["fct_orders"]
    mock_get_connector.return_value = fake

    anomaly = {
        "table": "fct_orders",
        "severity": Severity.WARNING,
        "type": "schema_drift",
        "message": "column `customer_id` type changed",
    }
    with patch("scherlok.cli._watch_table") as mock_watch:
        mock_watch.return_value = ([anomaly], {"row_count": 100})
        result = runner.invoke(
            app,
            [
                "dbt",
                "--project-dir", str(PG_PROJECT),
                "--connection-string", "postgresql://u:p@h/d",
                "--select", "fct_orders",
            ],
        )

    assert result.exit_code == 0, result.output  # WARNING does not fail by default
    # No downstream suffix because fct_orders has no descendants in the fixture
    assert "Affects" not in anomaly["message"]
