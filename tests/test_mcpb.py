"""Tests for the Claude Desktop bundle in `mcpb/`.

The bundle is published separately from the Python package but must describe
exactly what the package provides: the same version, the same tools, the same
entry point. Nothing here needs the `mcpb` CLI or a network — these are the
drift guards a manifest validator cannot give us.

The bundle's `pyproject.toml` is read as text rather than parsed: `tomllib` is
3.11+ and this package still supports 3.10.
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
LOCKFILE_PATH = BUNDLE_DIR / "uv.lock"

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


def test_bundle_pins_this_exact_release(bundle_pyproject):
    """`uv run --locked` installs whatever is pinned here, so it must be this version."""
    # `scherlok-mcpb` is the bundle's own project name, so the requirement is
    # matched on the `==` rather than on the prefix alone.
    pins = re.findall(r'"scherlok(?:\[[^\]]*\])?==([^",\s]+)"', bundle_pyproject)
    assert len(pins) == 1, f"expected exactly one pinned scherlok requirement, got {pins}"
    assert pins[0] == __version__
    assert f'version = "{__version__}"' in bundle_pyproject


def test_lockfile_agrees_with_the_pin():
    lock = LOCKFILE_PATH.read_text()
    assert f'name = "scherlok"\nversion = "{__version__}"' in lock, (
        "uv.lock is stale — run `uv lock` in mcpb/ after a version bump"
    )


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


def test_install_is_locked(manifest):
    args = manifest["server"]["mcp_config"]["args"]
    assert "--locked" in args, "without --locked, uv may resolve past the tested lockfile"
    assert LOCKFILE_PATH.is_file()
