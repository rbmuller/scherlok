"""Every place that states the release version must state the same one.

A release touches the version in several files, and each miss has shipped a
real defect: v1.0.0 never reached PyPI because `pyproject.toml` kept the old
number, and after 1.0.3 the README still told dbt users to install v1.0.2.
The Claude Desktop bundle has its own guards in tests/test_mcpb.py.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from scherlok import __version__

REPO = Path(__file__).resolve().parents[1]


def _read(relative: str) -> str:
    return (REPO / relative).read_text()


def test_pyproject_version():
    assert re.search(r'^version = "([^"]+)"', _read("pyproject.toml"), re.M).group(1) == __version__


def test_dbt_project_version():
    assert re.search(r"^version: '([^']+)'", _read("dbt_project.yml"), re.M).group(1) == __version__


def test_mcp_registry_manifest_versions():
    """server.json carries the version twice: the listing and the PyPI package it points at."""
    server = json.loads(_read("server.json"))
    assert server["version"] == __version__
    assert [package["version"] for package in server["packages"]] == [__version__]


@pytest.mark.parametrize("doc", ["README.md", "website/dbt-package.md"])
def test_dbt_package_install_snippets_pin_this_release(doc):
    """The `git:` install snippet is what dbt users copy; it must point at this tag."""
    revisions = re.findall(r"revision: (v\S+)", _read(doc))
    assert revisions, f"{doc} has no `revision:` pin to check"
    assert set(revisions) == {f"v{__version__}"}
