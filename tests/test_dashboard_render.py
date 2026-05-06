"""End-to-end render tests + golden HTML snapshot for dashboard regressions."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scherlok.dashboard.render import render_dashboard
from scherlok.detector.severity import Severity
from scherlok.store.sqlite import ProfileStore


@pytest.fixture
def store_with_data(tmp_path: Path):
    s = ProfileStore(db_path=tmp_path / "profiles.db")
    s.save_profile("orders", "volume", {"row_count": 1000, "timestamp": "2026-04-30T12:00:00Z"})
    s.save_profile("orders", "schema", {"columns": [{"name": "id", "type": "int"}]})
    s.save_profile("orders", "freshness", {})
    s.save_anomalies([
        {"table": "orders", "type": "volume_drop", "severity": Severity.CRITICAL,
         "message": "Row count dropped 60.0% (1000 -> 400)"},
    ])
    yield s
    s.close()


def test_render_returns_html_string(store_with_data):
    html = render_dashboard(store_with_data, project_name="testproj")
    assert html.startswith("<!DOCTYPE html>")
    assert "testproj" in html
    assert "Row count dropped" in html


def test_render_includes_logo_data_uri(store_with_data):
    html = render_dashboard(store_with_data)
    # Logo is embedded as base64 (no http fetch needed)
    assert "data:image/png;base64," in html


def test_render_invalid_theme_raises(store_with_data):
    with pytest.raises(ValueError, match="theme must be one of"):
        render_dashboard(store_with_data, theme="rainbow")


def test_render_dbt_context_renders_when_provided(store_with_data):
    ctx = {
        "project_name": "jaffle_shop",
        "adapter": "postgres",
        "models_count": 3,
        "sources_count": 1,
    }
    html = render_dashboard(store_with_data, dbt_context=ctx)
    assert "jaffle_shop" in html
    assert "dbt-card" in html  # dbt context section uses this CSS class


def test_render_dbt_context_omitted_when_none(store_with_data):
    """The dbt-card div only renders when dbt_context is provided."""
    html = render_dashboard(store_with_data, dbt_context=None)
    assert "<div class=\"dbt-card\">" not in html


def test_render_anomaly_trend_renders(tmp_path: Path):
    store = ProfileStore(db_path=tmp_path / "profiles.db")
    try:
        for days_ago, severity in [
            (0, Severity.WARNING),
            (2, Severity.CRITICAL),
            (5, Severity.WARNING),
            (7, Severity.INFO),
        ]:
            store.save_anomalies([
                {
                    "table": "orders",
                    "type": "volume_drop",
                    "severity": severity,
                    "message": f"Anomaly from {days_ago} days ago",
                },
            ])
            detected_at = (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat()
            store._conn.execute(  # noqa: SLF001
                "UPDATE anomalies SET detected_at = ? WHERE id = (SELECT MAX(id) FROM anomalies)",
                (detected_at,),
            )
            store._conn.commit()  # noqa: SLF001

        html = render_dashboard(store)
    finally:
        store.close()

    assert 'class="trend"' in html
    assert 'class="trend-svg"' in html
    # default 14-day window emits 15 bars (cutoff date inclusive)
    assert html.count('class="trend-bar') == 15
    assert 'class="trend-bar critical"' in html
    assert 'class="trend-bar warning"' in html
    assert 'class="trend-bar info"' in html


def test_render_redacts_password(store_with_data):
    html = render_dashboard(
        store_with_data,
        connection_string="postgresql://alice:supersecret@host/demo",
    )
    assert "supersecret" not in html
    assert "***" in html


def test_render_shows_first_seen_in_incident_header(store_with_data):
    """Regression: incident header must show 'first seen' timestamp."""
    html = render_dashboard(store_with_data)
    assert "first seen" in html


def test_render_self_contained_no_external_urls(store_with_data):
    """Dashboard must work offline — no <link>, no <script src>, no <img src=http>.

    The footer's GitHub link is the single allowed external href.
    """
    html = render_dashboard(store_with_data)
    assert 'src="http' not in html
    assert html.count('href="http') <= 1  # at most the footer's GitHub link
    assert "<link " not in html
    assert "<script src" not in html


def test_render_size_under_one_megabyte(store_with_data):
    """Spec acceptance criterion: file < 1 MB."""
    html = render_dashboard(store_with_data)
    assert len(html.encode("utf-8")) < 1_000_000
