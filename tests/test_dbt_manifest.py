"""Tests for scherlok.dbt.manifest — manifest.json parser."""

from pathlib import Path

import pytest

from scherlok.dbt.manifest import (
    discover_exposures,
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


def test_discover_exposures_parses_label_type_and_owner_email():
    manifest = load_manifest(PG_PROJECT)
    exposures = discover_exposures(manifest)

    assert len(exposures) == 1
    exposure = exposures[0]
    assert exposure.unique_id == "exposure.jaffle_shop.revenue_dashboard"
    assert exposure.name == "revenue_dashboard"
    assert exposure.label == "Revenue Dashboard"
    assert exposure.display_name == "Revenue Dashboard"
    assert exposure.exposure_type == "dashboard"
    assert exposure.owner_name == "Analytics Team"
    assert exposure.owner_emails == ("analytics-team@company.com",)


def test_discover_exposures_falls_back_to_name_without_label():
    manifest = {
        "exposures": {
            "exposure.demo.named_dashboard": {
                "name": "named_dashboard",
                "type": "dashboard",
            }
        }
    }

    exposure = discover_exposures(manifest)[0]

    assert exposure.label is None
    assert exposure.display_name == "named_dashboard"


@pytest.mark.parametrize(
    ("email", "expected"),
    [
        ("owner@example.com", ("owner@example.com",)),
        (["z@example.com", "a@example.com", "z@example.com"], ("a@example.com", "z@example.com")),
    ],
)
def test_discover_exposures_normalizes_single_and_list_owner_emails(email, expected):
    manifest = {
        "exposures": {
            "exposure.demo.dashboard": {
                "name": "dashboard",
                "type": "dashboard",
                "owner": {"email": email},
            }
        }
    }

    assert discover_exposures(manifest)[0].owner_emails == expected


def test_discover_exposures_handles_missing_owner_email():
    manifest = {
        "exposures": {
            "exposure.demo.dashboard": {
                "name": "dashboard",
                "type": "dashboard",
                "owner": {"name": "Analytics Team"},
            }
        }
    }

    exposure = discover_exposures(manifest)[0]

    assert exposure.owner_name == "Analytics Team"
    assert exposure.owner_emails == ()


def test_discover_exposures_empty_when_none():
    manifest = load_manifest(SF_PROJECT)
    assert discover_exposures(manifest) == []


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


def _manifest_with_snapshot() -> dict:
    """Build a minimal manifest containing one model and one snapshot."""
    return {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
            "adapter_type": "postgres",
        },
        "nodes": {
            "model.demo.fct_orders": {
                "resource_type": "model",
                "name": "fct_orders",
                "alias": "fct_orders",
                "database": "demo_db",
                "schema": "public",
                "relation_name": '"demo_db"."public"."fct_orders"',
                "config": {"materialized": "table"},
            },
            "snapshot.demo.orders_snapshot": {
                "resource_type": "snapshot",
                "name": "orders_snapshot",
                "alias": "orders_snapshot",
                "database": "demo_db",
                "schema": "snapshots",
                "relation_name": '"demo_db"."snapshots"."orders_snapshot"',
                "config": {"materialized": "snapshot"},
            },
        },
        "sources": {},
    }


def test_discover_models_skips_snapshots_by_default():
    manifest = _manifest_with_snapshot()
    models = discover_models(manifest)
    names = {m.name for m in models}
    assert "fct_orders" in names
    assert "orders_snapshot" not in names


def test_discover_models_include_snapshots_picks_up_snapshot_nodes():
    manifest = _manifest_with_snapshot()
    models = discover_models(manifest, include_snapshots=True)
    names = {m.name for m in models}
    assert names == {"fct_orders", "orders_snapshot"}

    snap = next(m for m in models if m.name == "orders_snapshot")
    assert snap.resource_type == "snapshot"
    assert snap.materialized == "snapshot"
    assert snap.database == "demo_db"
    assert snap.schema == "snapshots"
    assert snap.identifier == "orders_snapshot"
    assert snap.adapter == "postgres"


def test_discover_models_include_snapshots_no_op_when_no_snapshots():
    """include_snapshots=True does not change behavior when no snapshots exist."""
    manifest = load_manifest(PG_PROJECT)
    default = discover_models(manifest)
    expanded = discover_models(manifest, include_snapshots=True)
    assert {m.unique_id for m in default} == {m.unique_id for m in expanded}
