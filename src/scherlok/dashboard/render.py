"""Render the dashboard view-model into a self-contained HTML string."""

from __future__ import annotations

import base64
from pathlib import Path

import jinja2

from scherlok.dashboard.assembler import _humanize_iso, assemble_view
from scherlok.store.sqlite import ProfileStore

MODULE_DIR = Path(__file__).parent
TEMPLATE_PATH = MODULE_DIR / "template.html"
STYLES_PATH = MODULE_DIR / "styles.css"

# Logo lives at repo root, two levels up from this module
LOGO_PATH = MODULE_DIR.parent.parent.parent / "assets" / "scherlok-logo-96.png"

VALID_THEMES = {"auto", "dark", "light"}


def render_dashboard(
    store: ProfileStore,
    days: int = 14,
    theme: str = "auto",
    project_name: str = "scherlok",
    connection_string: str = "",
    dbt_context: dict | None = None,
) -> str:
    """Render the full dashboard HTML as a string.

    Args:
        store: ProfileStore to read from.
        days: Anomaly history window.
        theme: 'auto', 'dark', or 'light'.
        project_name: Display name for the project (header).
        connection_string: Connection URL (will be redacted before rendering).
        dbt_context: Optional dict with dbt project metadata. When None,
            the dbt context section is omitted.
    """
    if theme not in VALID_THEMES:
        raise ValueError(f"theme must be one of {sorted(VALID_THEMES)}, got {theme!r}")

    view = assemble_view(
        store,
        days=days,
        project_name=project_name,
        connection_string=connection_string,
        dbt_context=dbt_context,
    )

    css = STYLES_PATH.read_text(encoding="utf-8")
    logo_data_uri = _logo_data_uri()
    symptom_count = sum(len(i.symptoms) for i in view["incidents"])

    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(MODULE_DIR)),
        autoescape=jinja2.select_autoescape(["html"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env.filters["humanize_iso"] = _humanize_iso
    template = env.get_template("template.html")

    return template.render(
        **view,
        css=css,
        theme=theme,
        logo_data_uri=logo_data_uri,
        symptom_count=symptom_count,
    )


def _logo_data_uri() -> str:
    """Return a base64 data: URI for the embedded logo, or empty if missing."""
    if not LOGO_PATH.is_file():
        return ""
    payload = base64.b64encode(LOGO_PATH.read_bytes()).decode("ascii")
    return f"data:image/png;base64,{payload}"
