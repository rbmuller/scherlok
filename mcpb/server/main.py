"""Entry point for the Scherlok MCP bundle.

The bundle is a thin wrapper: `uv run` resolves `scherlok` from the pinned
dependencies in ../pyproject.toml, and the server itself lives in the package
(`scherlok.mcp.server`), so the desktop extension and `pip install scherlok`
expose exactly the same tools.
"""

from scherlok.mcp.server import main

if __name__ == "__main__":
    main()
