"""Tests for the Claude Desktop bundle in `mcpb/`.

The bundle is published separately from the Python package but must describe
exactly what the package provides: the same version, the same tools, the same
entry point. Nothing here needs the `mcpb` CLI or a network — these are the
drift guards a manifest validator cannot give us.

The bundle carries no lockfile: it is packed during the release that publishes
the version it depends on, so the requirement is a floor that these tests keep
equal to the package version.

Its `pyproject.toml` is read as text rather than parsed: `tomllib` is 3.11+ and
this package still supports 3.10.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from scherlok import __version__

BUNDLE_DIR = Path(__file__).resolve().parents[1] / "mcpb"
MANIFEST_PATH = BUNDLE_DIR / "manifest.json"
PYPROJECT_PATH = BUNDLE_DIR / "pyproject.toml"

pytestmark = pytest.mark.skipif(not MANIFEST_PATH.is_file(), reason="bundle not present")


@pytest.fixture(scope="module")
def manifest() -> dict:
    return json.loads(MANIFEST_PATH.read_text())


@pytest.fixture(scope="module")
def bundle_pyproject() -> str:
    return PYPROJECT_PATH.read_text()


def test_manifest_version_matches_the_package(manifest):
    """A bundle advertising a version the package doesn't have misleads users."""
    assert manifest["version"] == __version__


def test_bundle_requires_this_release_or_later(bundle_pyproject):
    """The floor must be this version: a lower one would install a stale server."""
    # `scherlok-mcpb` is the bundle's own project name, so the requirement is
    # matched on the `>=` rather than on the prefix alone.
    floors = re.findall(r'"scherlok(?:\[[^\]]*\])?>=([^",\s]+)', bundle_pyproject)
    assert len(floors) == 1, f"expected exactly one scherlok requirement, got {floors}"
    assert floors[0] == __version__
    assert f'version = "{__version__}"' in bundle_pyproject
    assert "<2" in bundle_pyproject, "cap the major so a 2.x release cannot break installs"


def test_manifest_tools_match_the_registered_tools(manifest):
    """The store listing shows these names; they must be the tools we serve."""
    from scherlok.mcp.server import _TOOLS

    assert {tool["name"] for tool in manifest["tools"]} == {fn.__name__ for fn in _TOOLS}
    assert all(tool["description"].strip() for tool in manifest["tools"])


def test_entry_point_exists_and_starts_the_packaged_server(manifest):
    entry_point = BUNDLE_DIR / manifest["server"]["entry_point"]
    assert entry_point.is_file()
    assert "from scherlok.mcp.server import main" in entry_point.read_text(), (
        "the bundle must run the packaged server, not a copy of it"
    )


def test_connection_is_user_supplied_and_marked_sensitive(manifest):
    """The connection string is a credential: user-configured, never a tool argument."""
    config = manifest["user_config"]["connection_string"]
    assert config["sensitive"] is True
    assert config["required"] is True
    assert manifest["server"]["mcp_config"]["env"]["SCHERLOK_CONNECTION"] == (
        "${user_config.connection_string}"
    )


def test_server_runs_from_the_bundle_directory(manifest):
    args = manifest["server"]["mcp_config"]["args"]
    assert args[:3] == ["run", "--directory", "${__dirname}"]
    assert "--locked" not in args, "the bundle ships no lockfile; --locked would fail at launch"
    assert not (BUNDLE_DIR / "uv.lock").exists(), (
        "a committed lockfile could only pin the previous release — see mcpb/pyproject.toml"
    )
