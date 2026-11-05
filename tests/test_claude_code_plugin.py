"""The Claude Code plugin's files hold together and hold no credentials (#113).

`claude plugin validate` is the authoritative check of the manifests; it needs
the Claude Code CLI, so these tests guard what can drift without it: the
hook points at a script that exists, the marketplace points at the plugin,
the skill keeps its rules, and nothing in the integration carries a secret.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
PLUGIN = REPO / "integrations" / "claude-code"
PLUGIN_ROOT_VAR = "${CLAUDE_PLUGIN_ROOT}"
SKILL = PLUGIN / "skills" / "verify-data-pipeline" / "SKILL.md"
AGENTS_SNIPPET = REPO / "integrations" / "AGENTS.snippet.md"
DEMO = REPO / "examples" / "claude_code_demo"

# A URL with a password, a credential assignment, or a key-shaped token.
CREDENTIAL_PATTERNS = (
    re.compile(r"\w+://[^/\s:@'\"]+:[^@\s'\"<>*]+@"),
    re.compile(r"(?i)\b(password|passwd|secret|token|api_key)\s*[:=]\s*['\"]?[^\s'\"<>*]{4,}"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{16,}"),
)


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _frontmatter(path: Path) -> dict[str, str]:
    text = path.read_text(encoding="utf-8")
    assert text.startswith("---\n"), "frontmatter must open on the first line"
    block = text.split("---\n", 2)[1]
    return dict(line.split(": ", 1) for line in block.strip().splitlines())


def test_manifest_names_the_plugin():
    manifest = _json(PLUGIN / ".claude-plugin" / "plugin.json")
    assert manifest["name"] == "scherlok"
    assert "version" not in manifest  # git-sourced: the commit is the version


def test_marketplace_lists_the_plugin_by_its_own_name():
    marketplace = _json(REPO / ".claude-plugin" / "marketplace.json")
    (entry,) = marketplace["plugins"]
    assert (REPO / entry["source"]).resolve() == PLUGIN.resolve()
    assert entry["name"] == _json(PLUGIN / ".claude-plugin" / "plugin.json")["name"]


def test_only_the_manifest_lives_in_dot_claude_plugin():
    assert [p.name for p in (PLUGIN / ".claude-plugin").iterdir()] == ["plugin.json"]


def test_hook_runs_the_bundled_script_after_bash():
    hooks = _json(PLUGIN / "hooks" / "hooks.json")["hooks"]
    assert list(hooks) == ["PostToolUse"]
    (group,) = hooks["PostToolUse"]
    assert group["matcher"] == "Bash"
    (handler,) = group["hooks"]
    assert handler["type"] == "command"
    (script,) = handler["args"]
    assert script.startswith(f"{PLUGIN_ROOT_VAR}/")
    assert (PLUGIN / script.removeprefix(f"{PLUGIN_ROOT_VAR}/")).is_file()


def test_hook_timeout_leaves_room_for_the_scripts_own_budget():
    """Claude Code cancels at `timeout` and drops the output; the script must finish first."""
    import importlib.util

    script = PLUGIN / "scripts" / "verify_after_dbt.py"
    spec = importlib.util.spec_from_file_location("verify_after_dbt", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    handler = _json(PLUGIN / "hooks" / "hooks.json")["hooks"]["PostToolUse"][0]["hooks"][0]
    assert handler["timeout"] > module.DEFAULT_TIMEOUT_SECONDS


def test_skill_frontmatter():
    meta = _frontmatter(SKILL)
    assert meta["name"] == SKILL.parent.name
    assert meta["description"]
    assert set(meta) <= {"name", "description", "allowed-tools"}  # Agent Skills spec fields
    granted = re.findall(r"Bash\(([^)]*)\)", meta["allowed-tools"])
    assert granted == ["scherlok verify *", "scherlok reconcile *"]  # read-only commands only


@pytest.mark.parametrize("path", [SKILL, AGENTS_SNIPPET])
def test_instructions_leave_accepting_a_baseline_to_a_person(path):
    text = " ".join(path.read_text(encoding="utf-8").split())
    assert "Never run `scherlok accept`" in text
    assert "ask a person" in text
    assert "scherlok verify --dbt-project" in text
    assert "scherlok reconcile" in text


def test_the_agents_snippet_does_not_mention_the_hook():
    assert "hook" not in AGENTS_SNIPPET.read_text(encoding="utf-8").lower()


TEXT_SUFFIXES = {".md", ".json", ".py", ".yml", ".sql", ".csv"}
# What running the demo or the tests leaves behind, all gitignored.
GENERATED_DIRS = {"__pycache__", ".venv", "target", "logs", "dbt_packages"}


def _integration_files() -> list[Path]:
    roots = [PLUGIN, AGENTS_SNIPPET, DEMO, REPO / ".claude-plugin"]
    files = []
    for root in roots:
        files.extend([root] if root.is_file() else root.rglob("*"))
    return sorted(
        path for path in files
        if path.suffix in TEXT_SUFFIXES and not GENERATED_DIRS & set(path.parts)
    )


@pytest.mark.parametrize("leak", [
    "SCHERLOK_CONNECTION=postgresql://analyst:s3cr3t@db.example.com/shop",
    "      password: s3cr3t-value",
    'api_key = "abcd1234efgh"',
    "ANTHROPIC_API_KEY=sk-ant-api03-abcdefghijklmnop",
])
def test_the_credential_patterns_catch_a_leak(leak):
    assert any(pattern.search(leak) for pattern in CREDENTIAL_PATTERNS)


@pytest.mark.parametrize("path", _integration_files(), ids=lambda p: str(p.relative_to(REPO)))
def test_no_credentials_in_the_integration(path):
    text = path.read_text(encoding="utf-8")
    for pattern in CREDENTIAL_PATTERNS:
        assert not pattern.search(text), f"{pattern.pattern} matches in {path}"
