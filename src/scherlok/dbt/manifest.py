"""Parse dbt's target/manifest.json to discover models and sources."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

MIN_MANIFEST_VERSION = 10  # dbt 1.6+
MATERIALIZED_PHYSICAL = {"table", "incremental", "view", "materialized_view"}
SUPPORTED_ADAPTERS = {"postgres", "bigquery", "snowflake"}


@dataclass(frozen=True)
class DbtNode:
    """A dbt model or source resolved to a physical relation."""

    unique_id: str
    name: str
    resource_type: str  # "model" or "source"
    materialized: str  # for sources: "source"
    database: str
    schema: str
    identifier: str  # alias for models, identifier for sources
    relation_name: str | None  # quoted FQN as dbt produced it (may be None)
    adapter: str  # postgres / bigquery / snowflake


def load_manifest(project_dir: str | Path) -> dict:
    """Load and validate target/manifest.json from a dbt project directory.

    Args:
        project_dir: Path to the dbt project root (must contain target/manifest.json).

    Raises:
        FileNotFoundError: When manifest.json is missing.
        ValueError: When manifest_version is too old or adapter is unsupported.
    """
    project_path = Path(project_dir)
    manifest_path = project_path / "target" / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(
            f"manifest.json not found at {manifest_path}. "
            f"Run `dbt compile` or `dbt run` first."
        )

    with manifest_path.open("r", encoding="utf-8") as f:
        manifest = json.load(f)

    version = _parse_manifest_version(manifest)
    if version < MIN_MANIFEST_VERSION:
        raise ValueError(
            f"Unsupported manifest_version v{version}. "
            f"Scherlok requires dbt 1.6+ (manifest v{MIN_MANIFEST_VERSION}+). "
            f"Upgrade dbt and re-run `dbt compile`."
        )

    adapter = manifest.get("metadata", {}).get("adapter_type")
    if adapter and adapter not in SUPPORTED_ADAPTERS:
        raise ValueError(
            f"Unsupported dbt adapter '{adapter}'. "
            f"Supported: {sorted(SUPPORTED_ADAPTERS)}. "
            f"Use --connection-string to bypass profiles.yml resolution."
        )

    return manifest


def discover_models(manifest: dict, include_snapshots: bool = False) -> list[DbtNode]:
    """Return all materialized models (table/incremental/view) from the manifest.

    Args:
        manifest: Loaded manifest.json dict.
        include_snapshots: When True, also include snapshot nodes. Snapshots are
            SCD Type 2 tables that physically exist in the warehouse and are
            profilable like any other materialized model. Defaults to False to
            preserve the v0 behavior of skipping snapshots.
    """
    adapter = manifest.get("metadata", {}).get("adapter_type", "")
    nodes = manifest.get("nodes", {})
    models: list[DbtNode] = []

    for unique_id, node in nodes.items():
        resource_type = node.get("resource_type")
        materialized = node.get("config", {}).get("materialized", "")

        if resource_type == "model":
            if materialized not in MATERIALIZED_PHYSICAL:
                continue
        elif resource_type == "snapshot" and include_snapshots:
            # dbt snapshots set config.materialized to "snapshot". The underlying
            # physical relation is a table, so they are profilable like any other
            # materialized model. We preserve the original value so callers can
            # distinguish snapshots from regular models via DbtNode.materialized.
            pass
        else:
            continue

        identifier = node.get("alias") or node.get("name", "")
        models.append(
            DbtNode(
                unique_id=unique_id,
                name=node.get("name", ""),
                resource_type=resource_type,
                materialized=materialized,
                database=node.get("database", "") or "",
                schema=node.get("schema", "") or "",
                identifier=identifier,
                relation_name=node.get("relation_name"),
                adapter=adapter,
            )
        )

    return models


def discover_sources(manifest: dict) -> list[DbtNode]:
    """Return all sources from the manifest."""
    adapter = manifest.get("metadata", {}).get("adapter_type", "")
    sources_dict = manifest.get("sources", {})
    sources: list[DbtNode] = []

    for unique_id, src in sources_dict.items():
        identifier = src.get("identifier") or src.get("name", "")
        sources.append(
            DbtNode(
                unique_id=unique_id,
                name=src.get("name", ""),
                resource_type="source",
                materialized="source",
                database=src.get("database", "") or "",
                schema=src.get("schema", "") or "",
                identifier=identifier,
                relation_name=src.get("relation_name"),
                adapter=adapter,
            )
        )

    return sources


def _parse_manifest_version(manifest: dict) -> int:
    """Extract integer version from metadata.dbt_schema_version URL.

    Examples:
        "https://schemas.getdbt.com/dbt/manifest/v10.json" -> 10
        "https://schemas.getdbt.com/dbt/manifest/v12.json" -> 12
    """
    raw = manifest.get("metadata", {}).get("dbt_schema_version", "")
    # Format: .../manifest/v<N>.json
    try:
        tail = raw.rsplit("/", 1)[-1]  # "v10.json"
        num = tail.lstrip("v").split(".", 1)[0]  # "10"
        return int(num)
    except (ValueError, IndexError):
        return 0
