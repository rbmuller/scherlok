"""dbt integration: parse manifest.json and run investigate/watch per model.

Reuses existing connectors (Postgres/BigQuery/Snowflake). No runtime dependency
on dbt-core — only reads JSON/YAML files produced by dbt.
"""

from scherlok.dbt.lineage import (
    build_dependency_graph,
    display_name,
    downstream_of,
    invert_graph,
    render_lineage_tree,
    upstream_of,
)
from scherlok.dbt.manifest import (
    DbtExposure,
    DbtNode,
    discover_exposures,
    discover_models,
    discover_sources,
    load_manifest,
)
from scherlok.dbt.profiles import ProfileResolutionError, resolve_connection_string
from scherlok.dbt.run_results import load_run_results, successful_model_unique_ids

__all__ = [
    "DbtNode",
    "DbtExposure",
    "ProfileResolutionError",
    "build_dependency_graph",
    "discover_exposures",
    "discover_models",
    "discover_sources",
    "display_name",
    "downstream_of",
    "invert_graph",
    "load_manifest",
    "load_run_results",
    "render_lineage_tree",
    "resolve_connection_string",
    "successful_model_unique_ids",
    "upstream_of",
]
