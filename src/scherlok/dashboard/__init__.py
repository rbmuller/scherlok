"""HTML dashboard generator: ProfileStore -> single-file report.html.

Read-only consumer of the local SQLite store. No connection required.
"""

from scherlok.dashboard.render import render_dashboard

__all__ = ["render_dashboard"]
