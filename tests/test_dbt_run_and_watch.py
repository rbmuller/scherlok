"""Tests for `scherlok dbt-run-and-watch` CLI command."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from scherlok.cli import app

runner = CliRunner()
FIXTURES = Path(__file__).parent / "fixtures" / "dbt"
PG_PROJECT = FIXTURES / "jaffle_shop_postgres"


def test_dbt_run_and_watch_help():
    result = runner.invoke(app, ["dbt-run-and-watch", "--help"])
    assert result.exit_code == 0
    # Help output gets wrapped to terminal width by Rich; normalize whitespace
    # before checking the contract sentence.
    output_normalized = " ".join(result.output.split())
    assert "Run `dbt run`" in output_normalized
    assert "exits with the same code WITHOUT running scherlok" in output_normalized


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
        "webhook", "email", "json_mode",
    ):
        assert required in kwargs, f"_dbt_impl call missing kwarg: {required}"
    assert kwargs["json_mode"] is False  # wrapper always runs the human-readable path


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
