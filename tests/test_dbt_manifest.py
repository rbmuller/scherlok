"""Tests for scherlok.dbt.manifest — manifest.json parser."""

from pathlib import Path

import pytest

from scherlok.dbt.manifest import (
    discover_models,
    discover_sources,
    load_manifest,
)

FIXTURES = Path(__file__).parent / "fixtures" / "dbt"
PG_PROJECT = FIXTURES / "jaffle_shop_postgres"
SF_PROJECT = FIXTURES / "jaffle_shop_snowflake"
OLD_PROJECT = FIXTURES / "jaffle_shop_old"


def test_load_manifest_valid():
    manifest = load_manifest(PG_PROJECT)
    assert manifest["metadata"]["adapter_type"] == "postgres"
    assert "nodes" in manifest


def test_load_manifest_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError, match="manifest.json not found"):
        load_manifest(tmp_path)


def test_load_manifest_old_version_rejected():
    with pytest.raises(ValueError, match="manifest_version v8"):
        load_manifest(OLD_PROJECT)


def test_discover_models_filters_to_materialized():
    """Ephemeral models, tests, and seeds must NOT be discovered."""
    manifest = load_manifest(PG_PROJECT)
    models = discover_models(manifest)
    names = {m.name for m in models}

    assert "stg_customers" in names  # view
    assert "stg_orders" in names  # view
    assert "fct_orders" in names  # table
    assert "dim_customers_inc" in names  # incremental

    assert "int_orders_pivoted" not in names  # ephemeral — skipped
    assert "unique_stg_customers_id" not in names  # test — skipped
    assert "raw_customers" not in names  # seed — skipped


def test_discover_models_postgres_naming():
    manifest = load_manifest(PG_PROJECT)
    models = discover_models(manifest)
    fct = next(m for m in models if m.name == "fct_orders")
    assert fct.adapter == "postgres"
    assert fct.schema == "public"
    assert fct.identifier == "fct_orders"
    assert fct.materialized == "table"


def test_discover_models_snowflake_quoted():
    """Snowflake aliases are typically uppercase."""
    manifest = load_manifest(SF_PROJECT)
    models = discover_models(manifest)
    assert len(models) == 1
    m = models[0]
    assert m.adapter == "snowflake"
    assert m.identifier == "STG_ORDERS"
    assert m.name == "stg_orders"
    assert m.schema == "PUBLIC"


def test_discover_sources():
    manifest = load_manifest(PG_PROJECT)
    sources = discover_sources(manifest)
    assert len(sources) == 1
    s = sources[0]
    assert s.resource_type == "source"
    assert s.identifier == "raw_payments"
    assert s.name == "payments"
    assert s.schema == "raw"


def test_discover_sources_empty_when_none():
    manifest = load_manifest(SF_PROJECT)
    assert discover_sources(manifest) == []


def test_load_manifest_unsupported_adapter(tmp_path):
    """An adapter outside postgres/bigquery/snowflake should raise."""
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(
        '{"metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",'
        '"adapter_type": "redshift"}, "nodes": {}, "sources": {}}'
    )
    with pytest.raises(ValueError, match="Unsupported dbt adapter 'redshift'"):
        load_manifest(tmp_path)
