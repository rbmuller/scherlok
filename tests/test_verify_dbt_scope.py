"""`scherlok verify --dbt-project`: verify only what the last dbt run built (#113).

Runs against a real DuckDB file with manifest.json and run_results.json
written the way dbt writes them, so the model-to-table matching is the one
`dbt-run-and-watch` uses. ~/.scherlok is the per-test scratch directory set up
by the autouse fixture in conftest.py.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from scherlok.cli import app
from scherlok.dbt import DEFAULT_TARGET_PATH, built_models, load_manifest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("duckdb") is None, reason="duckdb not installed"
)

runner = CliRunner(env={"NO_COLOR": "1"})

PROJECT = "shop"
MANIFEST_SCHEMA = "https://schemas.getdbt.com/dbt/manifest/v12.json"
ROWS = 200


def _sql(path: Path, *statements: str) -> None:
    import duckdb

    conn = duckdb.connect(str(path))
    try:
        for statement in statements:
            conn.execute(statement)
    finally:
        conn.close()


def _orders(name: str, rows: int = ROWS) -> str:
    return (
        f"CREATE OR REPLACE TABLE {name} AS SELECT i AS order_id, i * 1.5 AS amount "
        f"FROM range(1, {rows + 1}) t(i)"
    )


def _model(name: str, materialized: str = "table", alias: str | None = None) -> dict:
    return {
        "resource_type": "model", "name": name, "alias": alias or name,
        "database": "warehouse", "schema": "main", "config": {"materialized": materialized},
    }


def _write_artifacts(
    project: Path,
    nodes: dict[str, dict],
    results: dict[str, str],
    target: str = DEFAULT_TARGET_PATH,
    adapter: str = "duckdb",
) -> None:
    """manifest.json and run_results.json, keyed by model name, as dbt writes them."""
    target_dir = project / target
    target_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "metadata": {"dbt_schema_version": MANIFEST_SCHEMA, "adapter_type": adapter},
        "nodes": {f"model.{PROJECT}.{name}": node for name, node in nodes.items()},
        "sources": {},
    }
    run_results = {
        "results": [
            {"unique_id": f"model.{PROJECT}.{name}", "status": status}
            for name, status in results.items()
        ]
    }
    (target_dir / "manifest.json").write_text(json.dumps(manifest))
    (target_dir / "run_results.json").write_text(json.dumps(run_results))


@pytest.fixture
def warehouse(tmp_path, monkeypatch) -> Path:
    """Three tables with a baseline, reachable through SCHERLOK_CONNECTION."""
    path = tmp_path / "warehouse.duckdb"
    _sql(path, _orders("stg_orders"), _orders("fct_orders"), _orders("legacy_orders"))
    monkeypatch.setenv("SCHERLOK_CONNECTION", f"duckdb:///{path}")
    assert runner.invoke(app, ["investigate"]).exit_code == 0
    return path


@pytest.fixture
def project(tmp_path) -> Path:
    path = tmp_path / PROJECT
    path.mkdir()
    return path


def _verify(*args: str):
    result = runner.invoke(app, ["verify", "--output", "json", *args])
    payload = json.loads(result.stdout) if result.stdout.strip() else None
    return result, payload


def test_only_models_that_succeeded_are_verified(warehouse, project):
    _sql(warehouse, _orders("fct_orders", rows=20), _orders("legacy_orders", rows=20))
    _write_artifacts(
        project,
        {name: _model(name) for name in ("stg_orders", "fct_orders", "legacy_orders")},
        {"stg_orders": "success", "fct_orders": "success", "legacy_orders": "error"},
    )
    result, payload = _verify("--dbt-project", str(project))

    assert result.exit_code == 1
    assert payload["tables_verified"] == ["stg_orders", "fct_orders"]
    assert {a["table"] for a in payload["anomalies"]} == {"fct_orders"}
    assert payload["dbt_models_not_found"] == []


def test_a_passing_dbt_scope_exits_0(warehouse, project):
    _write_artifacts(project, {"fct_orders": _model("fct_orders")}, {"fct_orders": "success"})
    result, payload = _verify("--dbt-project", str(project))
    assert result.exit_code == 0, result.output
    assert payload["passed"] is True
    assert payload["tables_verified"] == ["fct_orders"]


def test_a_run_that_built_nothing_verifies_nothing(warehouse, project):
    """Not every table: an empty scope must not fall back to the whole warehouse."""
    _sql(warehouse, _orders("legacy_orders", rows=5))
    _write_artifacts(project, {"fct_orders": _model("fct_orders")}, {"fct_orders": "skipped"})
    result, payload = _verify("--dbt-project", str(project))

    assert result.exit_code == 0, result.output
    assert payload["tables_verified"] == payload["tables_without_baseline"] == []
    assert payload["anomalies"] == []
    runs = json.loads(runner.invoke(app, ["history", "--runs", "--output", "json"]).stdout)
    assert runs[0]["tables"] == []


def test_models_are_matched_by_alias(warehouse, project):
    _write_artifacts(
        project, {"orders_v2": _model("orders_v2", alias="fct_orders")}, {"orders_v2": "success"}
    )
    _, payload = _verify("--dbt-project", str(project))
    assert payload["tables_verified"] == ["fct_orders"]


def test_a_model_the_connection_cannot_see_is_reported_not_failed(warehouse, project):
    _write_artifacts(
        project,
        {"fct_orders": _model("fct_orders"), "int_orders": _model("int_orders")},
        {"fct_orders": "success", "int_orders": "success"},
    )
    result, payload = _verify("--dbt-project", str(project))
    assert result.exit_code == 0
    assert payload["dbt_models_not_found"] == ["int_orders"]

    text = runner.invoke(app, ["verify", "--dbt-project", str(project)])
    assert "dbt models not on this connection, not verified (1): int_orders" in text.output


def test_a_model_built_in_another_schema_is_not_the_table_of_its_name(warehouse, project):
    """#68: dbt built staging.fct_orders; the connection lists main.fct_orders, which
    the old match by bare name verified in its place."""
    staging_model = {**_model("fct_orders"), "schema": "staging"}
    _write_artifacts(project, {"fct_orders": staging_model}, {"fct_orders": "success"})

    result, payload = _verify("--dbt-project", str(project))

    assert result.exit_code == 0
    assert payload["tables_verified"] == []
    assert payload["dbt_models_not_found"] == ["fct_orders"]


def test_scherlok_dbt_reads_a_duckdb_project(warehouse, project):
    """#68: the manifest's adapter is no longer checked against an allowlist."""
    _write_artifacts(project, {"fct_orders": _model("fct_orders")}, {"fct_orders": "success"})
    (project / "dbt_project.yml").write_text(f"name: {PROJECT}\nprofile: {PROJECT}\n")

    result = runner.invoke(app, [
        "dbt", "--project-dir", str(project),
        "--connection-string", f"duckdb:///{warehouse}", "--output", "json",
    ])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert [model["physical"] for model in payload["models"]] == ["fct_orders"]
def test_a_dbt_build_is_verified_against_production(tmp_path, project):
    """The CI examples build into another database and verify what dbt built there
    against production's baseline (#88, #89): --dbt-project with --against."""
    production, build = tmp_path / "production.duckdb", tmp_path / "build.duckdb"
    _sql(production, _orders("fct_orders"))
    _sql(build, _orders("fct_orders", rows=ROWS // 4))
    investigated = runner.invoke(
        app, ["investigate"], env={"SCHERLOK_CONNECTION": f"duckdb:///{production}"}
    )
    assert investigated.exit_code == 0
    _write_artifacts(project, {"fct_orders": _model("fct_orders")}, {"fct_orders": "success"})

    result = runner.invoke(
        app,
        ["verify", "--output", "json", "--dbt-project", str(project),
         "--against", f"duckdb:///{production}"],
        env={"SCHERLOK_CONNECTION": f"duckdb:///{build}"},
    )

    payload = json.loads(result.stdout)
    assert result.exit_code == 1
    assert payload["tables_verified"] == ["fct_orders"]
    assert [a["type"] for a in payload["anomalies"]] == ["volume_drop"]


def test_ephemeral_models_are_skipped(warehouse, project):
    _write_artifacts(
        project, {"eph": _model("eph", materialized="ephemeral")}, {"eph": "success"}
    )
    _, payload = _verify("--dbt-project", str(project))
    assert payload["dbt_models_not_found"] == []
    assert payload["tables_verified"] == []


def test_a_new_model_counts_as_just_built(warehouse, project):
    """Like --table: an empty table a run just created fails the default gate."""
    _sql(warehouse, "CREATE TABLE new_orders (order_id INTEGER)")
    _write_artifacts(project, {"new_orders": _model("new_orders")}, {"new_orders": "success"})
    result, payload = _verify("--dbt-project", str(project))
    assert result.exit_code == 1
    assert payload["tables_without_baseline"] == ["new_orders"]
    assert [a["severity"] for a in payload["anomalies"]] == ["CRITICAL"]


def test_table_and_dbt_project_add_up(warehouse, project):
    _write_artifacts(project, {"fct_orders": _model("fct_orders")}, {"fct_orders": "success"})
    _, payload = _verify(
        "--dbt-project", str(project), "--table", "legacy_orders", "--table", "fct_orders"
    )
    assert payload["tables_verified"] == ["legacy_orders", "fct_orders"]


@pytest.mark.parametrize("absolute", [False, True])
def test_a_custom_target_path(warehouse, project, absolute):
    target = project / "build" if absolute else "build"
    _write_artifacts(
        project, {"fct_orders": _model("fct_orders")}, {"fct_orders": "success"}, target="build"
    )
    result, payload = _verify("--dbt-project", str(project), "--dbt-target-path", str(target))
    assert result.exit_code == 0, result.output
    assert payload["tables_verified"] == ["fct_orders"]


@pytest.mark.parametrize("missing", ["manifest.json", "run_results.json"])
def test_missing_artifacts_exit_1_with_stdout_left_empty(warehouse, project, missing):
    _write_artifacts(project, {"fct_orders": _model("fct_orders")}, {"fct_orders": "success"})
    (project / DEFAULT_TARGET_PATH / missing).unlink()
    result = runner.invoke(app, ["verify", "--output", "json", "--dbt-project", str(project)])
    assert result.exit_code == 1
    assert result.stdout == ""
    assert f"{missing} not found" in " ".join(result.stderr.split())


def test_malformed_run_results_exit_1(warehouse, project):
    _write_artifacts(project, {"fct_orders": _model("fct_orders")}, {"fct_orders": "success"})
    (project / DEFAULT_TARGET_PATH / "run_results.json").write_text("{not json")
    result = runner.invoke(app, ["verify", "--dbt-project", str(project)])
    assert result.exit_code == 1
    assert "Malformed run_results.json" in " ".join(result.output.split())


def test_without_dbt_project_the_json_keeps_its_keys(warehouse):
    _, payload = _verify()
    assert "dbt_models_not_found" not in payload


def test_built_models_reads_any_adapter(project):
    """verify brings its own connection, so a DuckDB or MySQL project is fine."""
    _write_artifacts(
        project, {"fct_orders": _model("fct_orders")}, {"fct_orders": "success"}, adapter="mysql"
    )
    assert [node.name for node in built_models(project)] == ["fct_orders"]
    assert load_manifest(project)["metadata"]["adapter_type"] == "mysql"
