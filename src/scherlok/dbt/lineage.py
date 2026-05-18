"""dbt model lineage from manifest.json parent_map / child_map.

The manifest stores the dependency graph in two forms:
  - parent_map: unique_id -> list of upstream unique_ids
  - child_map:  unique_id -> list of downstream unique_ids

Both are present in dbt 1.5+. We trust parent_map as the source of truth and
derive children on demand so we are robust to manifests where child_map is
missing or out of sync.
"""

from __future__ import annotations

from collections import deque


def build_dependency_graph(manifest: dict) -> dict[str, list[str]]:
    """Return the upstream dependency graph: unique_id -> list of parents.

    The returned mapping is a defensive shallow copy so callers can mutate it
    without affecting the manifest. Keys are every node the manifest knows
    about (models, sources, seeds, tests), not just materialized models.
    """
    parent_map = manifest.get("parent_map") or {}
    return {k: list(v) for k, v in parent_map.items()}


def upstream_of(graph: dict[str, list[str]], unique_id: str) -> list[str]:
    """Transitive closure of ancestors. BFS, deterministic per parent order.

    Excludes ``unique_id`` itself. Returns ``[]`` when the node is unknown
    or has no parents.
    """
    seen: set[str] = set()
    out: list[str] = []
    q = deque(graph.get(unique_id, []))
    while q:
        nid = q.popleft()
        if nid in seen:
            continue
        seen.add(nid)
        out.append(nid)
        q.extend(graph.get(nid, []))
    return out


def downstream_of(graph: dict[str, list[str]], unique_id: str) -> list[str]:
    """Transitive closure of descendants. Inverts ``graph`` on demand.

    For a single query the O(V+E) inversion is cheap; callers running many
    downstream queries should invert once via ``invert_graph()``.
    """
    return _walk(invert_graph(graph), unique_id)


def invert_graph(graph: dict[str, list[str]]) -> dict[str, list[str]]:
    """Return the children-of-X view: unique_id -> list of direct children."""
    children: dict[str, list[str]] = {}
    for child, parents in graph.items():
        for p in parents:
            children.setdefault(p, []).append(child)
    return children


def _walk(adj: dict[str, list[str]], start: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    q = deque(adj.get(start, []))
    while q:
        nid = q.popleft()
        if nid in seen:
            continue
        seen.add(nid)
        out.append(nid)
        q.extend(adj.get(nid, []))
    return out


def display_name(unique_id: str) -> str:
    """Strip the ``<resource_type>.<project>.`` prefix from a dbt unique_id.

    ``model.jaffle_shop.fct_orders`` -> ``fct_orders``
    ``source.jaffle_shop.raw.payments`` -> ``raw.payments``
    """
    # unique_ids are dot-separated; the first two segments are
    # <resource_type>.<project>. Everything after that is the actual name
    # (sources have an extra source-name segment).
    parts = unique_id.split(".", 2)
    return parts[2] if len(parts) == 3 else unique_id


def render_lineage_tree(
    graph: dict[str, list[str]],
    unique_id: str,
    max_depth: int = 3,
) -> str:
    """Render upstream + downstream lineage as an ASCII tree.

    Format:
        Upstream of <name>:
          <name>
          ├── parent_a
          │   └── grandparent_a
          └── parent_b
        Downstream of <name>:
          <name>
          ├── child_a
          │   └── grandchild_a
          └── child_b

    ``max_depth`` caps recursion to avoid runaway output on deep DAGs.
    Sections are omitted when the node has no parents / no children.
    """
    children = invert_graph(graph)
    name = display_name(unique_id)
    sections: list[str] = []

    if graph.get(unique_id):
        sections.append(f"Upstream of {name}:")
        sections.append(f"  {name}")
        sections.append(_render_subtree(graph, unique_id, max_depth, indent="  "))

    if children.get(unique_id):
        sections.append(f"Downstream of {name}:")
        sections.append(f"  {name}")
        sections.append(_render_subtree(children, unique_id, max_depth, indent="  "))

    return "\n".join(s for s in sections if s)


def _render_subtree(
    adj: dict[str, list[str]],
    root: str,
    max_depth: int,
    indent: str,
) -> str:
    """Recursive ASCII renderer using ├── / └── / │ pattern.

    ``adj`` is the adjacency list to walk (upstream graph or its inverse).
    ``root`` is the node whose children we are about to render. ``indent``
    is the prefix carried down from the parent so vertical bars line up.
    """
    lines: list[str] = []

    def visit(node: str, prefix: str, depth: int) -> None:
        kids = adj.get(node, [])
        if not kids or depth >= max_depth:
            if kids and depth >= max_depth:
                lines.append(f"{prefix}└── … ({len(kids)} more)")
            return
        last_idx = len(kids) - 1
        for i, kid in enumerate(kids):
            is_last = i == last_idx
            branch = "└── " if is_last else "├── "
            lines.append(f"{prefix}{branch}{display_name(kid)}")
            child_prefix = prefix + ("    " if is_last else "│   ")
            visit(kid, child_prefix, depth + 1)

    visit(root, indent, depth=0)
    return "\n".join(lines)
