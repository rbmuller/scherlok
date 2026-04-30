"""Resolve a Scherlok connection string from a dbt project's profiles.yml.

Supports the three adapters Scherlok already speaks: postgres, bigquery, snowflake.
Renders only `{{ env_var('FOO') }}` / `{{ env_var('FOO', 'default') }}` — anything
else (full Jinja, secrets:// resolvers, etc.) raises and asks the user to pass
--connection-string explicitly.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

ENV_VAR_PATTERN = re.compile(
    r"""\{\{\s*env_var\(\s*['"]([^'"]+)['"](?:\s*,\s*['"]([^'"]*)['"])?\s*\)\s*\}\}"""
)


class ProfileResolutionError(ValueError):
    """Raised when profiles.yml cannot be resolved into a connection string."""


def resolve_connection_string(
    project_dir: str | Path,
    profiles_dir: str | Path | None = None,
    target: str | None = None,
) -> str:
    """Resolve the connection string for a dbt project.

    Reads dbt_project.yml to find the profile name, then looks it up in
    profiles.yml under the chosen target.

    Args:
        project_dir: Path to the dbt project root (must contain dbt_project.yml).
        profiles_dir: Where profiles.yml lives. Defaults to ~/.dbt.
        target: Override the default target from profiles.yml.

    Raises:
        ProfileResolutionError: When required files/keys are missing or the
            adapter is unsupported.
    """
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:
        raise ProfileResolutionError(
            "PyYAML not installed. Install with `pip install scherlok[dbt]` "
            "or pass --connection-string explicitly."
        ) from exc

    project_path = Path(project_dir)
    project_yml_path = project_path / "dbt_project.yml"
    if not project_yml_path.is_file():
        raise ProfileResolutionError(
            f"dbt_project.yml not found at {project_yml_path}."
        )

    with project_yml_path.open("r", encoding="utf-8") as f:
        project_yml = yaml.safe_load(f) or {}

    profile_name = project_yml.get("profile")
    if not profile_name:
        raise ProfileResolutionError(
            f"`profile:` key missing from {project_yml_path}."
        )

    profiles_path = _resolve_profiles_path(profiles_dir)
    if not profiles_path.is_file():
        raise ProfileResolutionError(
            f"profiles.yml not found at {profiles_path}. "
            f"Set DBT_PROFILES_DIR or pass --profiles-dir."
        )

    with profiles_path.open("r", encoding="utf-8") as f:
        profiles_yml = yaml.safe_load(f) or {}

    profile = profiles_yml.get(profile_name)
    if not profile:
        raise ProfileResolutionError(
            f"Profile '{profile_name}' not found in {profiles_path}."
        )

    chosen_target = target or profile.get("target")
    if not chosen_target:
        raise ProfileResolutionError(
            f"No target specified and profile '{profile_name}' has no default target."
        )

    outputs = profile.get("outputs", {})
    output = outputs.get(chosen_target)
    if not output:
        raise ProfileResolutionError(
            f"Target '{chosen_target}' not found under profile '{profile_name}'."
        )

    rendered = _render_env_vars(output)
    return _output_to_connection_string(rendered)


def _resolve_profiles_path(profiles_dir: str | Path | None) -> Path:
    """Return the absolute path to profiles.yml."""
    if profiles_dir:
        return Path(profiles_dir) / "profiles.yml"
    env_dir = os.environ.get("DBT_PROFILES_DIR")
    if env_dir:
        return Path(env_dir) / "profiles.yml"
    return Path.home() / ".dbt" / "profiles.yml"


def _render_env_vars(value: Any) -> Any:
    """Recursively render `{{ env_var('NAME', 'default') }}` in strings."""
    if isinstance(value, str):
        return ENV_VAR_PATTERN.sub(_replace_env_var, value)
    if isinstance(value, dict):
        return {k: _render_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_render_env_vars(v) for v in value]
    return value


def _replace_env_var(match: re.Match) -> str:
    name = match.group(1)
    default = match.group(2) or ""
    return os.environ.get(name, default)


def _output_to_connection_string(output: dict) -> str:
    """Map a rendered dbt output block to a Scherlok connection string."""
    adapter_type = output.get("type", "")

    if adapter_type == "postgres":
        return _postgres_connection_string(output)
    if adapter_type == "bigquery":
        return _bigquery_connection_string(output)
    if adapter_type == "snowflake":
        return _snowflake_connection_string(output)

    raise ProfileResolutionError(
        f"Unsupported dbt adapter '{adapter_type}'. "
        f"Supported: postgres, bigquery, snowflake. "
        f"Use --connection-string to bypass profiles.yml."
    )


def _postgres_connection_string(output: dict) -> str:
    user = output.get("user", "")
    password = output.get("password", "")
    host = output.get("host", "localhost")
    port = output.get("port", 5432)
    dbname = output.get("dbname") or output.get("database", "")
    if not user or not dbname:
        raise ProfileResolutionError(
            "Postgres profile missing required fields: user, dbname."
        )
    auth = f"{user}:{password}@" if password else f"{user}@"
    return f"postgresql://{auth}{host}:{port}/{dbname}"


def _bigquery_connection_string(output: dict) -> str:
    project = output.get("project") or output.get("database", "")
    dataset = output.get("dataset") or output.get("schema", "")
    if not project or not dataset:
        raise ProfileResolutionError(
            "BigQuery profile missing required fields: project, dataset."
        )
    return f"bigquery://{project}/{dataset}"


def _snowflake_connection_string(output: dict) -> str:
    account = output.get("account", "")
    database = output.get("database", "")
    schema = output.get("schema", "")
    if not account or not database or not schema:
        raise ProfileResolutionError(
            "Snowflake profile missing required fields: account, database, schema."
        )
    return f"snowflake://{account}/{database}/{schema}"
