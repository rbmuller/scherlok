"""MCP server for Scherlok — exposes data-quality checks to AI coding agents.

Ships built-in since v0.7.0: `pip install scherlok` installs the `scherlok-mcp`
console script alongside the `scherlok` CLI. The connection is resolved
server-side (env/config), never passed by the model. See `server.py` for the
tool surface.
"""

from scherlok.mcp.server import build_server, main

__all__ = ["build_server", "main"]
