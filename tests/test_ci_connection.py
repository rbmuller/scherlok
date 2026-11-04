"""`scherlok ci <url>` and `check <url>` profile the database they were given."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
from typer.testing import CliRunner

from scherlok.cli import app
from scherlok.service import connection_scope
from scherlok.store.sqlite import ProfileStore

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("duckdb") is None,
    reason="duckdb not installed",
)

runner = CliRunner(env={"NO_COLOR": "1"})


def _warehouse(path: Path, table: str) -> str:
    import duckdb

    conn = duckdb.connect(str(path))
    try:
        conn.execute(f"CREATE TABLE {table} AS SELECT i AS id FROM range(10) t(i)")
    finally:
        conn.close()
    return f"duckdb:///{path}"


@pytest.mark.parametrize("command", ["ci", "check"])
def test_the_argument_wins_over_scherlok_connection(
    command, tmp_path, monkeypatch, isolated_scherlok_dir
):
    from_env = _warehouse(tmp_path / "env.duckdb", "from_env")
    given = _warehouse(tmp_path / "given.duckdb", "given")
    monkeypatch.setenv("SCHERLOK_CONNECTION", from_env)

    result = runner.invoke(app, [command, given])

    assert result.exit_code == 0, result.output
    [run] = ProfileStore(db_path=isolated_scherlok_dir / "profiles.db").get_runs()
    assert run["tables"] == ["given"]
    assert run["scope"] == connection_scope(given)
