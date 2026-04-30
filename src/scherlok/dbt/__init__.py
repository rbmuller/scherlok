"""dbt integration: parse manifest.json and run investigate/watch per model.

Reuses existing connectors (Postgres/BigQuery/Snowflake). No runtime dependency
on dbt-core — only reads JSON/YAML files produced by dbt.
"""

from scherlok.dbt.manifest import DbtNode, discover_models, discover_sources, load_manifest
from scherlok.dbt.profiles import ProfileResolutionError, resolve_connection_string

__all__ = [
    "DbtNode",
    "ProfileResolutionError",
    "discover_models",
    "discover_sources",
    "load_manifest",
    "resolve_connection_string",
]
