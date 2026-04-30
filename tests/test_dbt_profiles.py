"""Tests for scherlok.dbt.profiles — connection string resolution from profiles.yml."""

import os
from pathlib import Path

import pytest

from scherlok.dbt.profiles import ProfileResolutionError, resolve_connection_string

FIXTURES = Path(__file__).parent / "fixtures" / "dbt"
PG_PROJECT = FIXTURES / "jaffle_shop_postgres"
SF_PROJECT = FIXTURES / "jaffle_shop_snowflake"

PROFILES_PG = FIXTURES / "profiles_postgres"
PROFILES_BQ = FIXTURES / "profiles_bigquery"
PROFILES_SF = FIXTURES / "profiles_snowflake"


def test_resolve_postgres_profile(monkeypatch):
    monkeypatch.setenv("PG_USER", "alice")
    monkeypatch.setenv("PG_PASSWORD", "wonderland")
    conn = resolve_connection_string(PG_PROJECT, profiles_dir=PROFILES_PG)
    assert conn == "postgresql://alice:wonderland@localhost:5432/jaffle_db"


def test_resolve_postgres_uses_env_var_default(monkeypatch):
    """env_var('PG_USER', 'jaffle') must fall back to the literal default."""
    monkeypatch.delenv("PG_USER", raising=False)
    monkeypatch.delenv("PG_PASSWORD", raising=False)
    conn = resolve_connection_string(PG_PROJECT, profiles_dir=PROFILES_PG)
    assert conn == "postgresql://jaffle:secret@localhost:5432/jaffle_db"


def test_resolve_postgres_target_override(monkeypatch):
    monkeypatch.delenv("PG_USER", raising=False)
    conn = resolve_connection_string(PG_PROJECT, profiles_dir=PROFILES_PG, target="prod")
    assert conn == "postgresql://prod_user:prod_pw@prod-host:5432/jaffle_prod"


def test_resolve_bigquery_profile(monkeypatch):
    # bigquery profile uses jaffle_shop_postgres dbt_project.yml (same `profile:` name)
    conn = resolve_connection_string(PG_PROJECT, profiles_dir=PROFILES_BQ)
    assert conn == "bigquery://my-gcp-project/jaffle_shop"


def test_resolve_snowflake_profile(monkeypatch):
    monkeypatch.setenv("SF_USER", "robson")
    monkeypatch.setenv("SF_PASSWORD", "elementary")
    conn = resolve_connection_string(SF_PROJECT, profiles_dir=PROFILES_SF)
    assert conn == "snowflake://my-account.us-east-1/ANALYTICS/PUBLIC"


def test_unsupported_adapter_raises(tmp_path):
    project = tmp_path / "proj"
    project.mkdir()
    (project / "dbt_project.yml").write_text("name: x\nprofile: x\n")
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(
        "x:\n  target: dev\n  outputs:\n    dev:\n      type: redshift\n      host: rs\n"
    )
    with pytest.raises(ProfileResolutionError, match="Unsupported dbt adapter 'redshift'"):
        resolve_connection_string(project, profiles_dir=profiles)


def test_missing_dbt_project_yml(tmp_path):
    with pytest.raises(ProfileResolutionError, match="dbt_project.yml not found"):
        resolve_connection_string(tmp_path, profiles_dir=PROFILES_PG)


def test_missing_profiles_yml(tmp_path):
    project = tmp_path / "proj"
    project.mkdir()
    (project / "dbt_project.yml").write_text("name: x\nprofile: x\n")
    with pytest.raises(ProfileResolutionError, match="profiles.yml not found"):
        resolve_connection_string(project, profiles_dir=tmp_path / "missing")


def test_dbt_profiles_dir_env_var(monkeypatch):
    """When --profiles-dir is None and DBT_PROFILES_DIR is set, the env var wins."""
    monkeypatch.setenv("DBT_PROFILES_DIR", str(PROFILES_PG))
    monkeypatch.setenv("PG_USER", "envalice")
    monkeypatch.setenv("PG_PASSWORD", "envpw")
    # profiles_dir=None, DBT_PROFILES_DIR is the resolution path
    conn = resolve_connection_string(PG_PROJECT, profiles_dir=None)
    assert conn == "postgresql://envalice:envpw@localhost:5432/jaffle_db"
    assert os.environ["DBT_PROFILES_DIR"]  # safety check


def test_target_not_found(tmp_path):
    project = tmp_path / "proj"
    project.mkdir()
    (project / "dbt_project.yml").write_text("name: x\nprofile: x\n")
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(
        "x:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: postgres\n"
        "      host: localhost\n"
        "      user: u\n"
        "      dbname: d\n"
    )
    with pytest.raises(ProfileResolutionError, match="Target 'staging' not found"):
        resolve_connection_string(project, profiles_dir=profiles, target="staging")
