"""Tests for `scherlok dbt-run-and-watch` CLI command."""

import json
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from scherlok.cli import app

runner = CliRunner()
FIXTURES = Path(__file__).parent / "fixtures" / "dbt"
PG_PROJECT = FIXTURES / "jaffle_shop_postgres"

# CI terminals emit ANSI styling (bold/dim) even when colors are disabled;
# strip them before substring assertions on Rich-rendered help text.
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _copy_project_with_run_results(
    tmp_path: Path, results: list[dict[str, str]], *, include_snapshot: bool = False
) -> Path:
    """Copy the manifest fixture and replace its execution artifact."""
    project_dir = tmp_path / "dbt_project"
    target_dir = project_dir / "target"
    target_dir.mkdir(parents=True)
    manifest = json.loads((PG_PROJECT / "target" / "manifest.json").read_text())
    if include_snapshot:
        manifest["nodes"]["snapshot.jaffle_shop.orders_snapshot"] = {
            "resource_type": "snapshot",
            "name": "orders_snapshot",
            "alias": "orders_snapshot",
            "database": "jaffle_db",
            "schema": "snapshots",
            "relation_name": '"jaffle_db"."snapshots"."orders_snapshot"',
            "config": {"materialized": "snapshot"},
        }
    (target_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (target_dir / "run_results.json").write_text(
        json.dumps({"results": results}), encoding="utf-8"
    )
    return project_dir


def _successful_result(unique_id: str, status: str = "success") -> dict[str, str]:
    return {"unique_id": unique_id, "status": status}


def test_dbt_run_and_watch_help():
    result = runner.invoke(app, ["dbt-run-and-watch", "--help"])
    assert result.exit_code == 0
    # Rich wraps to terminal width and injects ANSI between words; strip ANSI
    # then collapse whitespace before checking the contract sentences.
    output_clean = " ".join(ANSI_RE.sub("", result.output).split())
    assert "Run `dbt run`" in output_clean
    assert "exits with the same code WITHOUT running scherlok" in output_clean


def test_dbt_run_and_watch_missing_dbt_binary():
    """If `dbt` is not on PATH, exit with a clear error."""
    with patch("scherlok.cli.shutil.which", return_value=None):
        result = runner.invoke(
            app, ["dbt-run-and-watch", "--project-dir", str(PG_PROJECT)]
        )
    assert result.exit_code == 1
    assert "`dbt` binary not found on PATH" in result.output


def test_dbt_run_and_watch_propagates_dbt_run_failure():
    """When `dbt run` exits non-zero, scherlok must NOT run and the exit code must propagate."""
    fake_run = MagicMock()
    fake_run.return_value.returncode = 2

    with patch("scherlok.cli.shutil.which", return_value="/usr/local/bin/dbt"), \
         patch("scherlok.cli.subprocess.run", fake_run), \
         patch("scherlok.cli._dbt_impl") as mock_impl:
        result = runner.invoke(
            app,
            [
                "dbt-run-and-watch",
                "--project-dir", str(PG_PROJECT),
                "--target", "prod",
            ],
        )

    assert result.exit_code == 2
    assert mock_impl.call_count == 0  # scherlok dbt did NOT run
    # The dbt cmd we constructed must include the user's --target.
    cmd_args = fake_run.call_args.args[0]
    assert "/usr/local/bin/dbt" in cmd_args
    assert "run" in cmd_args
    assert "--target" in cmd_args
    assert "prod" in cmd_args


def test_dbt_run_and_watch_passes_select_through_to_dbt():
    """--select values must be forwarded to `dbt run` so it builds the right models."""
    fake_run = MagicMock()
    fake_run.return_value.returncode = 0

    with patch("scherlok.cli.shutil.which", return_value="/usr/local/bin/dbt"), \
         patch("scherlok.cli.subprocess.run", fake_run), \
         patch("scherlok.cli._dbt_impl") as mock_impl:
        result = runner.invoke(
            app,
            [
                "dbt-run-and-watch",
                "--project-dir", str(PG_PROJECT),
                "--connection-string", "postgresql://u:p@h/d",
                "--select", "stg_orders",
                "--select", "fct_orders",
            ],
        )

    assert result.exit_code == 0, result.output
    cmd_args = fake_run.call_args.args[0]
    # --select appears twice, each with its model name
    select_indices = [i for i, x in enumerate(cmd_args) if x == "--select"]
    assert len(select_indices) == 2
    selected_models = {cmd_args[i + 1] for i in select_indices}
    assert selected_models == {"stg_orders", "fct_orders"}
    # And scherlok dbt got the same select list afterwards
    assert mock_impl.call_count == 1
    assert mock_impl.call_args.kwargs["select"] == ["stg_orders", "fct_orders"]


@patch("scherlok.cli.get_connector")
def test_dbt_run_and_watch_profiles_only_successful_models(
    mock_get_connector, tmp_path, monkeypatch
):
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    project_dir = _copy_project_with_run_results(
        tmp_path,
        [
            _successful_result("model.jaffle_shop.stg_customers"),
            _successful_result("model.jaffle_shop.stg_orders", "error"),
            _successful_result("model.jaffle_shop.fct_orders", "skipped"),
        ],
    )
    fake_conn = MagicMock()
    fake_conn.connect.return_value = True
    fake_conn.list_tables.return_value = ["stg_customers", "stg_orders", "fct_orders"]
    mock_get_connector.return_value = fake_conn
    fake_run = MagicMock()
    fake_run.return_value.returncode = 0

    with patch("scherlok.cli.shutil.which", return_value="/usr/local/bin/dbt"), \
         patch("scherlok.cli.subprocess.run", fake_run), \
         patch("scherlok.cli._watch_table", return_value=([], {"row_count": 1})) as mock_watch:
        result = runner.invoke(
            app,
            [
                "dbt-run-and-watch",
                "--project-dir", str(project_dir),
                "--connection-string", "postgresql://u:p@h/d",
            ],
        )

    assert result.exit_code == 0, result.output
    assert [call.args[2] for call in mock_watch.call_args_list] == ["stg_customers"]


@patch("scherlok.cli.get_connector")
def test_dbt_run_and_watch_dbt_selector_is_not_reinterpreted(
    mock_get_connector, tmp_path, monkeypatch
):
    """The artifact, not a non-literal selector, controls executed models."""
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    project_dir = _copy_project_with_run_results(
        tmp_path, [_successful_result("model.jaffle_shop.fct_orders")]
    )
    fake_conn = MagicMock()
    fake_conn.connect.return_value = True
    fake_conn.list_tables.return_value = ["fct_orders"]
    mock_get_connector.return_value = fake_conn
    fake_run = MagicMock()
    fake_run.return_value.returncode = 0

    with patch("scherlok.cli.shutil.which", return_value="/usr/local/bin/dbt"), \
         patch("scherlok.cli.subprocess.run", fake_run), \
         patch("scherlok.cli._watch_table", return_value=([], {"row_count": 1})) as mock_watch:
        result = runner.invoke(
            app,
            [
                "dbt-run-and-watch",
                "--project-dir", str(project_dir),
                "--connection-string", "postgresql://u:p@h/d",
                "--select", "tag:orders",
            ],
        )

    assert result.exit_code == 0, result.output
    assert [call.args[2] for call in mock_watch.call_args_list] == ["fct_orders"]
    cmd_args = fake_run.call_args.args[0]
    assert "tag:orders" in cmd_args


@patch("scherlok.cli.get_connector")
def test_dbt_run_and_watch_empty_execution_set_does_not_profile_manifest(
    mock_get_connector, tmp_path, monkeypatch
):
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    project_dir = _copy_project_with_run_results(
        tmp_path, [_successful_result("model.jaffle_shop.fct_orders", "error")]
    )
    fake_run = MagicMock()
    fake_run.return_value.returncode = 0

    with patch("scherlok.cli.shutil.which", return_value="/usr/local/bin/dbt"), \
         patch("scherlok.cli.subprocess.run", fake_run), \
         patch("scherlok.cli._watch_table") as mock_watch:
        result = runner.invoke(
            app,
            [
                "dbt-run-and-watch",
                "--project-dir", str(project_dir),
                "--connection-string", "postgresql://u:p@h/d",
            ],
        )

    assert result.exit_code == 0, result.output
    assert "No successfully executed dbt models" in result.output
    mock_get_connector.assert_not_called()
    mock_watch.assert_not_called()


def test_dbt_run_and_watch_json_empty_execution_set_is_parseable(tmp_path):
    project_dir = _copy_project_with_run_results(
        tmp_path, [_successful_result("model.jaffle_shop.fct_orders", "no-op")]
    )
    fake_run = MagicMock()
    fake_run.return_value.returncode = 0

    with patch("scherlok.cli.shutil.which", return_value="/usr/local/bin/dbt"), \
         patch("scherlok.cli.subprocess.run", fake_run):
        result = runner.invoke(
            app,
            [
                "dbt-run-and-watch",
                "--project-dir", str(project_dir),
                "--output", "json",
            ],
        )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["models"] == []
    assert payload["summary"] == {
        "profiled": 0,
        "anomalies": 0,
        "critical": 0,
        "warning": 0,
    }


@pytest.mark.parametrize("artifact_kind", ["missing", "malformed"])
def test_dbt_run_and_watch_artifact_failure_skips_profiling(
    tmp_path, artifact_kind
):
    project_dir = _copy_project_with_run_results(tmp_path, [])
    artifact_path = project_dir / "target" / "run_results.json"
    if artifact_kind == "missing":
        artifact_path.unlink()
    else:
        artifact_path.write_text("{not json", encoding="utf-8")
    fake_run = MagicMock()
    fake_run.return_value.returncode = 0

    with patch("scherlok.cli.shutil.which", return_value="/usr/local/bin/dbt"), \
         patch("scherlok.cli.subprocess.run", fake_run), \
         patch("scherlok.cli._dbt_impl") as mock_impl:
        result = runner.invoke(
            app,
            [
                "dbt-run-and-watch",
                "--project-dir", str(project_dir),
                "--connection-string", "postgresql://u:p@h/d",
            ],
        )

    assert result.exit_code == 1
    assert "run_results.json" in result.output
    mock_impl.assert_not_called()


def test_dbt_run_and_watch_json_artifact_failure_is_parseable(tmp_path):
    project_dir = _copy_project_with_run_results(tmp_path, [])
    (project_dir / "target" / "run_results.json").unlink()
    fake_run = MagicMock()
    fake_run.return_value.returncode = 0

    with patch("scherlok.cli.shutil.which", return_value="/usr/local/bin/dbt"), \
         patch("scherlok.cli.subprocess.run", fake_run), \
         patch("scherlok.cli._dbt_impl") as mock_impl:
        result = runner.invoke(
            app,
            [
                "dbt-run-and-watch",
                "--project-dir", str(project_dir),
                "--output", "json",
            ],
        )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["returncode"] == 1
    assert "run_results.json" in payload["error"]
    mock_impl.assert_not_called()


@patch("scherlok.cli.get_connector")
def test_dbt_run_and_watch_keeps_explicit_sources_and_snapshots(
    mock_get_connector, tmp_path, monkeypatch
):
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    project_dir = _copy_project_with_run_results(
        tmp_path,
        [_successful_result("model.jaffle_shop.stg_customers")],
        include_snapshot=True,
    )
    fake_conn = MagicMock()
    fake_conn.connect.return_value = True
    fake_conn.list_tables.return_value = ["stg_customers", "raw_payments", "orders_snapshot"]
    mock_get_connector.return_value = fake_conn
    fake_run = MagicMock()
    fake_run.return_value.returncode = 0

    with patch("scherlok.cli.shutil.which", return_value="/usr/local/bin/dbt"), \
         patch("scherlok.cli.subprocess.run", fake_run), \
         patch("scherlok.cli._watch_table", return_value=([], {"row_count": 1})) as mock_watch:
        result = runner.invoke(
            app,
            [
                "dbt-run-and-watch",
                "--project-dir", str(project_dir),
                "--connection-string", "postgresql://u:p@h/d",
                "--include-sources",
                "--include-snapshots",
            ],
        )

    assert result.exit_code == 0, result.output
    assert {call.args[2] for call in mock_watch.call_args_list} == {
        "stg_customers", "raw_payments", "orders_snapshot"
    }


def test_dbt_run_and_watch_calls_scherlok_dbt_on_success():
    """When `dbt run` succeeds, `_dbt_impl` must be invoked with all required flags."""
    fake_run = MagicMock()
    fake_run.return_value.returncode = 0

    with patch("scherlok.cli.shutil.which", return_value="/usr/local/bin/dbt"), \
         patch("scherlok.cli.subprocess.run", fake_run), \
         patch("scherlok.cli._dbt_impl") as mock_impl:
        result = runner.invoke(
            app,
            [
                "dbt-run-and-watch",
                "--project-dir", str(PG_PROJECT),
                "--connection-string", "postgresql://u:p@h/d",
                "--target", "prod",
                "--fail-on", "warning",
            ],
        )

    assert result.exit_code == 0, result.output
    assert mock_impl.call_count == 1
    kwargs = mock_impl.call_args.kwargs
    assert kwargs["project_dir"] == str(PG_PROJECT)
    assert kwargs["target"] == "prod"
    assert kwargs["fail_on"] == "warning"
    assert kwargs["connection_string"] == "postgresql://u:p@h/d"
    # Every required keyword-only param of _dbt_impl must be passed explicitly,
    # otherwise the Typer-wrapped `dbt()` would receive OptionInfo sentinels
    # at runtime. Locking these in here so a future regression surfaces.
    for required in (
        "profiles_dir", "select", "include_sources", "include_snapshots",
        "webhook", "email", "json_mode", "show_lineage", "executed_model_ids",
    ):
        assert required in kwargs, f"_dbt_impl call missing kwarg: {required}"
    assert kwargs["json_mode"] is False  # default --output is 'text'
    assert kwargs["executed_model_ids"] == {
        "model.jaffle_shop.stg_customers",
        "model.jaffle_shop.stg_orders",
        "model.jaffle_shop.fct_orders",
        "model.jaffle_shop.dim_customers_inc",
    }


@patch("scherlok.cli.get_connector")
def test_dbt_run_and_watch_real_dbt_impl_path(mock_get_connector, tmp_path, monkeypatch):
    """Run with `_dbt_impl` not mocked so OptionInfo-sentinel-class bugs surface.

    The earlier tests `patch("scherlok.cli._dbt_impl")` which would mask the
    failure if any of `_dbt_impl`'s keyword-only params was passed as a Typer
    OptionInfo sentinel (the exact regression that prompted this rewrite).
    This test exercises the wrapper end-to-end past the mock boundary and
    asserts the real `_dbt_impl` returned without an AttributeError on
    `output.lower()` or a truthy-OptionInfo `include_snapshots` flip.
    """
    monkeypatch.setenv("HOME", str(tmp_path))

    fake_conn = MagicMock()
    fake_conn.connect.return_value = True
    fake_conn.list_tables.return_value = [
        "stg_customers", "stg_orders", "fct_orders", "dim_customers_inc",
    ]
    mock_get_connector.return_value = fake_conn

    fake_run = MagicMock()
    fake_run.return_value.returncode = 0

    with patch("scherlok.cli.shutil.which", return_value="/usr/local/bin/dbt"), \
         patch("scherlok.cli.subprocess.run", fake_run), \
         patch("scherlok.cli._watch_table") as mock_watch:
        mock_watch.return_value = ([], {"row_count": 1})
        result = runner.invoke(
            app,
            [
                "dbt-run-and-watch",
                "--project-dir", str(PG_PROJECT),
                "--connection-string", "postgresql://u:p@h/d",
            ],
        )

    # If `include_snapshots` or `json_mode` were leaking as OptionInfo
    # sentinels into the real `_dbt_impl`, this would have crashed on
    # `output.lower()` or behaved wrong on snapshot discovery. Clean exit
    # plus the real per-model loop running is the signal that the
    # keyword-only contract is intact end-to-end.
    assert result.exit_code == 0, result.output
    assert mock_watch.call_count == 4  # 4 materialized models in the fixture


def test_dbt_run_and_watch_output_invalid_value_rejected():
    """Unknown --output values exit 1 with a clear message, mirroring `scherlok dbt`."""
    result = runner.invoke(
        app,
        [
            "dbt-run-and-watch",
            "--project-dir", str(PG_PROJECT),
            "--output", "yaml",
        ],
    )
    assert result.exit_code == 1
    assert "Invalid --output" in result.output


def test_dbt_run_and_watch_json_mode_passes_json_mode_to_dbt_impl():
    """--output json must flip `_dbt_impl`'s json_mode, unlike the default text path."""
    fake_run = MagicMock()
    fake_run.return_value.returncode = 0

    with patch("scherlok.cli.shutil.which", return_value="/usr/local/bin/dbt"), \
         patch("scherlok.cli.subprocess.run", fake_run), \
         patch("scherlok.cli._dbt_impl") as mock_impl:
        result = runner.invoke(
            app,
            [
                "dbt-run-and-watch",
                "--project-dir", str(PG_PROJECT),
                "--connection-string", "postgresql://u:p@h/d",
                "--output", "json",
            ],
        )

    assert result.exit_code == 0, result.output
    assert mock_impl.call_count == 1
    assert mock_impl.call_args.kwargs["json_mode"] is True
    # dbt run's own stdout must be rerouted away from the inherited stdout fd
    # (CliRunner swaps in its own sys.stderr during invoke, so we can't compare
    # identity against sys.stderr here -- absence of the default is the signal),
    # so it can never land on the stdout channel reserved for the JSON payload.
    assert fake_run.call_args.kwargs["stdout"] is not None


def test_dbt_run_and_watch_json_mode_text_mode_leaves_dbt_stdout_inherited():
    """Without --output json, dbt run's stdout must stream normally (not rerouted)."""
    fake_run = MagicMock()
    fake_run.return_value.returncode = 0

    with patch("scherlok.cli.shutil.which", return_value="/usr/local/bin/dbt"), \
         patch("scherlok.cli.subprocess.run", fake_run), \
         patch("scherlok.cli._dbt_impl"):
        result = runner.invoke(
            app,
            [
                "dbt-run-and-watch",
                "--project-dir", str(PG_PROJECT),
                "--connection-string", "postgresql://u:p@h/d",
            ],
        )

    assert result.exit_code == 0, result.output
    assert fake_run.call_args.kwargs["stdout"] is None


@patch("scherlok.cli.get_connector")
def test_dbt_run_and_watch_json_mode_real_impl_emits_parseable_object(
    mock_get_connector, tmp_path, monkeypatch
):
    """End-to-end: --output json produces the same JSON shape as `scherlok dbt --output json`,
    with no Rich chatter (`Running:`, `dbt run succeeded`) leaking onto stdout."""
    monkeypatch.setenv("HOME", str(tmp_path))

    fake_conn = MagicMock()
    fake_conn.connect.return_value = True
    fake_conn.list_tables.return_value = [
        "stg_customers", "stg_orders", "fct_orders", "dim_customers_inc",
    ]
    mock_get_connector.return_value = fake_conn

    fake_run = MagicMock()
    fake_run.return_value.returncode = 0

    with patch("scherlok.cli.shutil.which", return_value="/usr/local/bin/dbt"), \
         patch("scherlok.cli.subprocess.run", fake_run), \
         patch("scherlok.cli._watch_table") as mock_watch:
        mock_watch.return_value = ([], {"row_count": 1})
        result = runner.invoke(
            app,
            [
                "dbt-run-and-watch",
                "--project-dir", str(PG_PROJECT),
                "--connection-string", "postgresql://u:p@h/d",
                "--output", "json",
            ],
        )

    assert result.exit_code == 0, result.output
    assert "Running:" not in result.stdout
    assert "dbt run succeeded" not in result.stdout
    payload = json.loads(result.stdout)
    assert payload["project_dir"] == str(PG_PROJECT)
    assert payload["adapter"] == "postgres"
    assert len(payload["models"]) == 4
    assert payload["summary"]["profiled"] == 4


def test_dbt_run_and_watch_json_mode_dbt_not_found_emits_json_error():
    """When dbt is not on PATH under --output json, stdout gets a JSON error document."""
    with patch("scherlok.cli.shutil.which", return_value=None):
        result = runner.invoke(
            app,
            [
                "dbt-run-and-watch",
                "--project-dir", str(PG_PROJECT),
                "--output", "json",
            ],
        )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["project_dir"] == str(PG_PROJECT)
    assert "not found" in payload["error"]
    assert payload["returncode"] == 1


def test_dbt_run_and_watch_json_mode_dbt_run_failure_emits_json_error():
    """When `dbt run` fails under --output json, stdout gets a JSON error document
    instead of the plain-text message, so CI parsers never see two formats."""
    fake_run = MagicMock()
    fake_run.return_value.returncode = 2

    with patch("scherlok.cli.shutil.which", return_value="/usr/local/bin/dbt"), \
         patch("scherlok.cli.subprocess.run", fake_run), \
         patch("scherlok.cli._dbt_impl") as mock_impl:
        result = runner.invoke(
            app,
            [
                "dbt-run-and-watch",
                "--project-dir", str(PG_PROJECT),
                "--output", "json",
            ],
        )

    assert result.exit_code == 2
    assert mock_impl.call_count == 0  # scherlok dbt did NOT run
    payload = json.loads(result.stdout)
    assert payload["project_dir"] == str(PG_PROJECT)
    assert payload["error"] == "dbt run failed"
    assert payload["returncode"] == 2
