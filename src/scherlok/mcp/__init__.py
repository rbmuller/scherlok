"""MCP server for Scherlok — exposes data-quality checks to AI coding agents.

Install with `pip install scherlok[mcp]` and run via the `scherlok-mcp`
console script. The connection is resolved server-side (env/config), never
passed by the model. See `server.py` for the tool surface.
"""

from scherlok.mcp.server import build_server, main

__all__ = ["build_server", "main"]
