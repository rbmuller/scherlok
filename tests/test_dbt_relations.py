"""#68: dbt models matched to the tables a connection can see, schema included."""

from scherlok.connectors.base import UNKNOWN_NAMESPACE, RelationNamespace
from scherlok.dbt.manifest import DbtNode
from scherlok.dbt.relations import ambiguous_identifiers, resolve_relation

MARTS = RelationNamespace(database="analytics", schema="marts")


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
    assert resolve_relation(_node("orders"), {"orders"}, MARTS) == "orders"


def test_a_model_in_another_schema_is_not_the_visible_table_of_the_same_name():
    """The old match by bare name profiled marts.orders for staging.orders."""
    staging = _node("orders", schema="staging")

    assert resolve_relation(staging, {"orders"}, MARTS) is None


def test_a_model_in_another_database_is_not_matched():
    assert resolve_relation(_node("orders", database="other"), {"orders"}, MARTS) is None


def test_schema_and_database_compare_case_insensitively():
    """Snowflake returns upper case; dbt writes what the project configured."""
    namespace = RelationNamespace(database="ANALYTICS", schema="MARTS")

    assert resolve_relation(_node("orders"), {"ORDERS"}, namespace) == "ORDERS"


def test_the_alias_wins_over_the_model_name():
    node = _node("fct_orders_v2", name="fct_orders")

    assert resolve_relation(node, {"fct_orders", "fct_orders_v2"}, MARTS) == "fct_orders_v2"


def test_an_unknown_database_compares_the_schema_only():
    """DuckDB and MySQL don't say which database dbt calls theirs."""
    namespace = RelationNamespace(schema="main")

    assert resolve_relation(_node("orders", schema="main", database="dev"), {"orders"},
                            namespace) == "orders"


def test_without_a_namespace_a_unique_name_still_matches():
    assert resolve_relation(_node("orders"), {"orders"}, UNKNOWN_NAMESPACE) == "orders"


def test_without_a_namespace_an_ambiguous_name_matches_nothing():
    marts, staging = _node("orders"), _node("orders", schema="staging")
    ambiguous = ambiguous_identifiers([marts, staging])

    assert ambiguous == {"orders"}
    assert resolve_relation(marts, {"orders"}, UNKNOWN_NAMESPACE, ambiguous) is None
    assert resolve_relation(staging, {"orders"}, UNKNOWN_NAMESPACE, ambiguous) is None


def test_with_a_namespace_an_ambiguous_name_matches_the_visible_schema():
    marts, staging = _node("orders"), _node("orders", schema="staging")
    ambiguous = ambiguous_identifiers([marts, staging])

    assert resolve_relation(marts, {"orders"}, MARTS, ambiguous) == "orders"
    assert resolve_relation(staging, {"orders"}, MARTS, ambiguous) is None


def test_the_same_relation_twice_is_not_ambiguous():
    """A model and a snapshot, or a repeated --select, can name one relation."""
    assert ambiguous_identifiers([_node("orders"), _node("orders")]) == set()
