"""The CI examples in website/ci-cd.md parse and gate the way the page says (#88, #89).

A copy-paste example that doesn't parse, or that passes the connection on the
command line, is worse than none.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

CI_DOCS = Path(__file__).resolve().parent.parent / "website" / "ci-cd.md"
YAML_BLOCK = re.compile(r"```yaml\n(.*?)```", re.DOTALL)
GITLAB = "GitLab CI"
CIRCLECI = "CircleCI"
CONNECTION_ON_COMMAND_LINE = re.compile(r"scherlok (ci|connect)\s+[\"']?\$")


def _section(title: str) -> str:
    text = CI_DOCS.read_text(encoding="utf-8")
    start = text.index(f"\n## {title}\n")
    end = text.find("\n## ", start + 1)
    return text[start:end]


def _blocks(title: str) -> list[dict]:
    return [yaml.safe_load(block) for block in YAML_BLOCK.findall(_section(title))]


def _scripts(job: dict) -> str:
    return "\n".join(job.get("script", []))


def test_gitlab_example_gates_merge_requests_and_learns_on_a_schedule():
    pipeline, dbt_job = _blocks(GITLAB)

    gate, baseline = pipeline["data-check"], pipeline["baseline"]
    assert "scherlok verify" in _scripts(gate)
    assert "merge_request_event" in gate["rules"][0]["if"]
    assert "scherlok watch" in _scripts(baseline)
    assert '"schedule"' in baseline["rules"][0]["if"]
    assert pipeline["variables"]["SCHERLOK_STORE"].startswith("s3://")
    assert "scherlok verify --dbt-project ." in _scripts(dbt_job["data-check"])


def test_circleci_example_gates_branches_and_learns_on_a_schedule():
    (config,) = _blocks(CIRCLECI)

    steps = config["jobs"]["data-check"]["steps"]
    assert any("scherlok verify" in str(step) for step in steps)
    assert any("scherlok watch" in str(step) for step in config["jobs"]["baseline"]["steps"])
    workflows = config["workflows"]
    assert "scheduled_pipeline" in str(workflows["learn"]["when"])
    assert "scheduled_pipeline" in str(workflows["gate"]["when"]["not"])


@pytest.mark.parametrize("title", [GITLAB, CIRCLECI])
def test_no_example_puts_the_connection_on_the_command_line(title):
    assert not CONNECTION_ON_COMMAND_LINE.search(_section(title))
    assert "SCHERLOK_CONNECTION" in _section(title)
