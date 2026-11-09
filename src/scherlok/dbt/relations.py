"""Match dbt nodes to the tables a connection can see (#68).

A connector lists bare table names of one database and schema (its
``namespace``). A node matches a listed table when its identifier (or name)
does and its schema and database are that namespace's, compared without case
where the connector knows them. A connector that can't say where it looks
matches by name alone, and only names that no two nodes in different schemas
share.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable

from scherlok.connectors.base import RelationNamespace
from scherlok.dbt.manifest import DbtNode


def ambiguous_identifiers(nodes: Iterable[DbtNode]) -> set[str]:
    """Identifiers, case-folded, that name relations in more than one schema or database."""
    places: dict[str, set[tuple[str, str]]] = {}
    for node in nodes:
        place = (node.database.casefold(), node.schema.casefold())
        places.setdefault(node.identifier.casefold(), set()).add(place)
    return {identifier for identifier, seen in places.items() if len(seen) > 1}


def resolve_relation(
    node: DbtNode,
    visible: Iterable[str],
    namespace: RelationNamespace,
    ambiguous: Collection[str] = (),
) -> str | None:
    """Return the listed table ``node`` is, as the connector spells it, or None."""
    if not _in_namespace(node, namespace, ambiguous):
        return None
    tables = list(visible)
    by_folded: dict[str, str] = {}
    for table in tables:
        by_folded.setdefault(table.casefold(), table)
    for candidate in (node.identifier, node.name):
        if not candidate:
            continue
        if candidate in tables:
            return candidate
        if candidate.casefold() in by_folded:
            return by_folded[candidate.casefold()]
    return None


def _in_namespace(
    node: DbtNode, namespace: RelationNamespace, ambiguous: Collection[str]
) -> bool:
    if namespace.schema is None:
        return node.identifier.casefold() not in ambiguous
    return _same(node.schema, namespace.schema) and _same(node.database, namespace.database)


def _same(node_part: str, namespace_part: str | None) -> bool:
    """Equal without case, or not comparable because either side is unknown."""
    if not node_part or namespace_part is None:
        return True
    return node_part.casefold() == namespace_part.casefold()
