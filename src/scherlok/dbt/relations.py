"""Match dbt nodes to the tables a connection can see (#68).

A connector lists bare table names of one database and schema (its
``namespace``). A node matches a listed table when its identifier does and its
schema and database are that namespace's, compared without case where the
connector knows them. A connector that can't say where it looks matches by
identifier alone, and only identifiers that no two relations of the manifest
share across schemas.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable

from scherlok.connectors.base import RelationNamespace
from scherlok.dbt.manifest import DbtNode, discover_models, discover_sources


def manifest_relations(manifest: dict) -> list[DbtNode]:
    """Every relation the manifest names: models, snapshots and sources.

    Ambiguity is judged over these, not over a selection, so a selected model
    can't take the table of an unselected one that shares its name.
    """
    return [*discover_models(manifest, include_snapshots=True), *discover_sources(manifest)]


def ambiguous_identifiers(nodes: Iterable[DbtNode]) -> set[str]:
    """Identifiers, case-folded, that name relations in more than one schema or database."""
    places: dict[str, set[tuple[str, str]]] = {}
    for node in nodes:
        place = (node.database.casefold(), node.schema.casefold())
        places.setdefault(node.identifier.casefold(), set()).add(place)
    return {identifier for identifier, seen in places.items() if len(seen) > 1}


class VisibleTables:
    """The tables a connection lists, indexed once for every node to match."""

    def __init__(self, names: Iterable[str]) -> None:
        self._names = set(names)
        spellings: dict[str, list[str]] = {}
        for name in self._names:
            spellings.setdefault(name.casefold(), []).append(name)
        # A match without case only when one listed name folds to it, so
        # "Orders" and "ORDERS" never stand in for each other.
        self._by_folded = {
            folded: names[0] for folded, names in spellings.items() if len(names) == 1
        }

    def find(self, identifier: str) -> str | None:
        """The listed name for ``identifier``: exact, else the one name equal without case."""
        if identifier in self._names:
            return identifier
        return self._by_folded.get(identifier.casefold())


def resolve_relation(
    node: DbtNode,
    visible: VisibleTables,
    namespace: RelationNamespace,
    ambiguous: Collection[str] = (),
) -> str | None:
    """Return the listed table ``node`` is, as the connector spells it, or None.

    Only the node's identifier counts: it is the relation dbt built (the
    alias of a model), so a table named after the model is a different one.
    """
    if not node.identifier or not _in_namespace(node, namespace, ambiguous):
        return None
    return visible.find(node.identifier)


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
