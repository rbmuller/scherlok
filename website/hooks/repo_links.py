"""MkDocs hook: include repository Markdown files and point their links at GitHub.

Several site pages are the repository's own Markdown files (CHANGELOG.md,
CONTRIBUTING.md, module READMEs). A page includes one with a line of the form

    --8<-- "src/scherlok/dbt/README.md"

(the pymdownx.snippets syntax, resolved here instead so link targets can be
rewritten before Markdown conversion, which is when MkDocs validates them).
Links relative to the repo root are right on GitHub and wrong on the site, so
they become GitHub URLs; links between site pages are left untouched. One
source of truth per document, no copies.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_BLOB = "https://github.com/rbmuller/scherlok/blob/main/"
REPO_TREE = "https://github.com/rbmuller/scherlok/tree/main/"

INCLUDE_RE = re.compile(r'^--8<-- "([^"]+)"\s*$', re.MULTILINE)

# Link targets that are neither absolute URLs, anchors, mailto nor site-root paths.
RELATIVE_LINK_RE = re.compile(r"\]\((?!https?://|mailto:|#|/)([^)\s]+)\)")


def _to_github(match: re.Match) -> str:
    target = match.group(1)
    if target.endswith(".md") and "/" not in target:
        return match.group(0)  # a site page, e.g. "ci-cd.md"
    base = REPO_TREE if target.endswith("/") else REPO_BLOB
    return f"]({base}{target})"


def _include(repo_root: Path) -> callable:
    def replace(match: re.Match) -> str:
        source = repo_root / match.group(1)
        if not source.is_file():
            raise FileNotFoundError(f"included file not found: {source}")
        return RELATIVE_LINK_RE.sub(_to_github, source.read_text(encoding="utf-8")).rstrip("\n")

    return replace


def on_page_markdown(markdown: str, page, config, files) -> str:
    repo_root = Path(config.config_file_path).resolve().parent
    return INCLUDE_RE.sub(_include(repo_root), markdown)
