"""#68: dbt models matched to the tables a connection can see, schema included."""

from scherlok.connectors.base import UNKNOWN_NAMESPACE, RelationNamespace
from scherlok.dbt.manifest import DbtNode
from scherlok.dbt.relations import (
    VisibleTables,
    ambiguous_identifiers,
    manifest_relations,
    resolve_relation,
)

MARTS = RelationNamespace(database="analytics", schema="marts")
ORDERS = VisibleTables({"orders"})


def _node(
    identifier: str,
    *,
    schema: str = "marts",
    database: str = "analytics",
    name: str | None = None,
) -> DbtNode:
    return DbtNode(
        unique_id=f"model.shop.{schema}_{identifier}",
        name=name or identifier,
        resource_type="model",
        materialized="table",
        database=database,
        schema=schema,
        identifier=identifier,
        relation_name=None,
        adapter="postgres",
    )


def test_a_model_in_the_visible_schema_matches_its_table():
    assert resolve_relation(_node("orders"), ORDERS, MARTS) == "orders"


def test_a_model_in_another_schema_is_not_the_visible_table_of_the_same_name():
    """The old match by bare name profiled marts.orders for staging.orders."""
    assert resolve_relation(_node("orders", schema="staging"), ORDERS, MARTS) is None


def test_a_model_in_another_database_is_not_matched():
    assert resolve_relation(_node("orders", database="other"), ORDERS, MARTS) is None


def test_schema_and_database_compare_case_insensitively():
    """Snowflake returns upper case; dbt writes what the project configured."""
    namespace = RelationNamespace(database="ANALYTICS", schema="MARTS")

    assert resolve_relation(_node("orders"), VisibleTables({"ORDERS"}), namespace) == "ORDERS"


def test_a_table_named_after_the_model_is_not_its_alias():
    """The alias is the relation dbt built; `fct_orders` here is a legacy table
    (#145 review)."""
    node = _node("fct_orders_v2", name="fct_orders")

    assert resolve_relation(node, VisibleTables({"fct_orders"}), MARTS) is None
    both = VisibleTables({"fct_orders", "fct_orders_v2"})
    assert resolve_relation(node, both, MARTS) == "fct_orders_v2"


def test_a_match_without_case_needs_one_listed_spelling():
    """"Orders" and "ORDERS" are two tables on a case-sensitive warehouse; which
    one a set yields first must not decide the match (#145 review)."""
    two_spellings = VisibleTables({"Orders", "ORDERS"})
    exact_and_upper = VisibleTables({"orders", "ORDERS"})

    assert resolve_relation(_node("orders"), two_spellings, MARTS) is None
    assert resolve_relation(_node("orders"), exact_and_upper, MARTS) == "orders"


def test_an_unknown_database_compares_the_schema_only():
    """DuckDB and MySQL don't say which database dbt calls theirs."""
    node = _node("orders", schema="main", database="dev")

    assert resolve_relation(node, ORDERS, RelationNamespace(schema="main")) == "orders"


def test_without_a_namespace_a_unique_name_still_matches():
    assert resolve_relation(_node("orders"), ORDERS, UNKNOWN_NAMESPACE) == "orders"


def test_without_a_namespace_an_ambiguous_name_matches_nothing():
    marts, staging = _node("orders"), _node("orders", schema="staging")
    ambiguous = ambiguous_identifiers([marts, staging])

    assert ambiguous == {"orders"}
    assert resolve_relation(marts, ORDERS, UNKNOWN_NAMESPACE, ambiguous) is None
    assert resolve_relation(staging, ORDERS, UNKNOWN_NAMESPACE, ambiguous) is None


def test_with_a_namespace_an_ambiguous_name_matches_the_visible_schema():
    marts, staging = _node("orders"), _node("orders", schema="staging")
    ambiguous = ambiguous_identifiers([marts, staging])

    assert resolve_relation(marts, ORDERS, MARTS, ambiguous) == "orders"
    assert resolve_relation(staging, ORDERS, MARTS, ambiguous) is None


def test_the_same_relation_twice_is_not_ambiguous():
    """A model and a snapshot, or a repeated --select, can name one relation."""
    assert ambiguous_identifiers([_node("orders"), _node("orders")]) == set()


def test_ambiguity_counts_every_relation_of_the_manifest():
    """A selection of one model can't make its name unambiguous: an unselected
    source in another schema shares it (#145 review)."""
    manifest = {
        "metadata": {"adapter_type": "postgres"},
        "nodes": {
            "model.shop.orders": {
                "resource_type": "model", "name": "orders", "database": "analytics",
                "schema": "marts", "config": {"materialized": "table"},
            },
        },
        "sources": {
            "source.shop.raw.orders": {
                "name": "orders", "database": "analytics", "schema": "raw",
            },
        },
    }

    assert ambiguous_identifiers(manifest_relations(manifest)) == {"orders"}
