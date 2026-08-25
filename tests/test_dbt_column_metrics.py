"""Deterministic rendering tests for the native dbt column metrics model."""

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment

MODEL_SQL = (
    Path(__file__).parents[1] / "models" / "scherlok_column_metrics.sql"
).read_text(encoding="utf-8")


@dataclass
class FakeColumn:
    name: str
    dtype: str


@dataclass
class FakeRelation:
    identifier: str

    def __str__(self) -> str:
        return f'"warehouse"."public"."{self.identifier}"'


class FakeAdapter:
    def __init__(self, columns_by_identifier: dict[str, list[FakeColumn]]) -> None:
        self.columns_by_identifier = columns_by_identifier
        self.relation_calls: list[str] = []
        self.column_calls: list[str] = []

    def get_relation(self, *, database: str, schema: str, identifier: str) -> FakeRelation:
        self.relation_calls.append(identifier)
        return FakeRelation(identifier)

    def get_columns_in_relation(self, relation: FakeRelation) -> list[FakeColumn]:
        self.column_calls.append(relation.identifier)
        return self.columns_by_identifier[relation.identifier]

    @staticmethod
    def quote(name: str) -> str:
        return f'"{name}"'


class FakeDbt:
    @staticmethod
    def type_float() -> str:
        return "double"

    @staticmethod
    def type_string() -> str:
        return "varchar"

    @staticmethod
    def current_timestamp() -> str:
        return "current_timestamp"


def model_node(name: str, package_name: str = "analytics") -> SimpleNamespace:
    return SimpleNamespace(
        resource_type="model",
        package_name=package_name,
        name=name,
        unique_id=f"model.{package_name}.{name}",
        database="warehouse",
        schema="public",
        alias=name,
        config=SimpleNamespace(materialized="table"),
    )


def make_test_node(
    name: str,
    *,
    attached_node: str | None = None,
    column_name: str | None = None,
    namespace: str | None = "scherlok",
    package_name: str = "analytics",
    dependency_ids: list[str] | None = None,
    kwargs_column_name: str | None = None,
) -> SimpleNamespace:
    metadata_kwargs = {}
    if kwargs_column_name is not None:
        metadata_kwargs["column_name"] = kwargs_column_name

    return SimpleNamespace(
        resource_type="test",
        package_name=package_name,
        name=name,
        unique_id=f"test.{package_name}.{name}",
        test_metadata=SimpleNamespace(
            name="null_anomaly",
            namespace=namespace,
            kwargs=metadata_kwargs,
        ),
        attached_node=attached_node,
        column_name=column_name,
        depends_on=SimpleNamespace(nodes=dependency_ids or []),
    )


def render_model(
    nodes: list[SimpleNamespace],
    columns_by_identifier: dict[str, list[FakeColumn]],
    *,
    include_only: list[str] | None = None,
    exclude_models: list[str] | None = None,
) -> tuple[str, FakeAdapter]:
    adapter = FakeAdapter(columns_by_identifier)
    environment = Environment(extensions=["jinja2.ext.do"])
    environment.globals.update(
        config=lambda **kwargs: "",
        dbt=FakeDbt(),
        none=None,
    )

    variables = {
        "scherlok_include_models": include_only or [],
        "scherlok_exclude_models": exclude_models or [],
    }

    rendered = environment.from_string(MODEL_SQL).render(
        adapter=adapter,
        graph=SimpleNamespace(
            nodes={node.unique_id: node for node in nodes if hasattr(node, "unique_id")}
        ),
        dbt=FakeDbt(),
        var=lambda name, default=None: variables.get(name, default),
    )
    return rendered, adapter


def test_profiles_only_tested_columns_and_preserves_column_type() -> None:
    orders = model_node("orders")
    test = make_test_node(
        "null_anomaly_orders_email",
        attached_node=orders.unique_id,
        column_name="email",
    )

    rendered, adapter = render_model(
        [orders, test],
        {
            "orders": [
                FakeColumn("id", "integer"),
                FakeColumn("email", "text"),
                FakeColumn("created_at", "timestamp"),
            ]
        },
    )

    assert adapter.relation_calls == ["orders"]
    assert adapter.column_calls == ["orders"]
    assert "'email' as column_name" in rendered
    assert "'text' as column_type" in rendered
    assert "'id' as column_name" not in rendered
    assert "'created_at' as column_name" not in rendered


def test_profiles_multiple_columns_and_ignores_unrelated_generic_tests() -> None:
    orders = model_node("orders")
    email_test = make_test_node(
        "null_anomaly_orders_email",
        attached_node=orders.unique_id,
        column_name="email",
    )
    status_test = make_test_node(
        "null_anomaly_orders_status",
        attached_node=orders.unique_id,
        column_name="status",
    )
    unrelated_test = make_test_node(
        "not_null_orders_id",
        attached_node=orders.unique_id,
        column_name="id",
    )
    unrelated_test.test_metadata.name = "not_null_proportion"
    other_package_test = make_test_node(
        "other_null_anomaly_orders_id",
        attached_node=orders.unique_id,
        column_name="id",
        namespace="other_package",
    )

    rendered, _ = render_model(
        [orders, email_test, status_test, unrelated_test, other_package_test],
        {
            "orders": [
                FakeColumn("id", "integer"),
                FakeColumn("email", "text"),
                FakeColumn("status", "text"),
            ]
        },
    )

    assert "'email' as column_name" in rendered
    assert "'status' as column_name" in rendered
    assert "'id' as column_name" not in rendered


def test_duplicate_tests_do_not_duplicate_profiling() -> None:
    orders = model_node("orders")
    tests = [
        make_test_node(
            f"null_anomaly_orders_email_{suffix}",
            attached_node=orders.unique_id,
            column_name="email",
        )
        for suffix in ("one", "two")
    ]

    rendered, _ = render_model(
        [orders, *tests],
        {"orders": [FakeColumn("email", "text")]},
    )

    assert rendered.count("'email' as column_name") == 1


def test_fallback_metadata_maps_test_to_its_model_and_column() -> None:
    orders = model_node("orders")
    metrics = model_node("scherlok_metrics", package_name="scherlok")
    fallback_test = make_test_node(
        "null_anomaly_orders_status",
        column_name=None,
        attached_node=None,
        namespace=None,
        package_name="scherlok",
        dependency_ids=[orders.unique_id, metrics.unique_id],
        kwargs_column_name="status",
    )

    rendered, adapter = render_model(
        [orders, metrics, fallback_test],
        {
            "orders": [FakeColumn("id", "integer"), FakeColumn("status", "varchar")],
            "scherlok_metrics": [FakeColumn("table_name", "varchar")],
        },
    )

    assert adapter.relation_calls == ["orders"]
    assert "'status' as column_name" in rendered
    assert "'varchar' as column_type" in rendered


def test_ambiguous_dependency_fallback_does_not_profile_a_model() -> None:
    orders = model_node("orders")
    customers = model_node("customers")
    fallback_test = make_test_node(
        "null_anomaly_ambiguous",
        column_name=None,
        attached_node=None,
        dependency_ids=[orders.unique_id, customers.unique_id],
        kwargs_column_name="email",
    )

    rendered, adapter = render_model(
        [orders, customers, fallback_test],
        {
            "orders": [FakeColumn("email", "text")],
            "customers": [FakeColumn("email", "text")],
        },
    )

    assert adapter.relation_calls == []
    assert "where 1 = 0" in rendered


def test_model_without_null_anomaly_test_is_not_profiled() -> None:
    orders = model_node("orders")
    users = model_node("users")
    test = make_test_node(
        "null_anomaly_orders_email",
        attached_node=orders.unique_id,
        column_name="email",
    )

    _, adapter = render_model(
        [orders, users, test],
        {
            "orders": [FakeColumn("email", "text")],
            "users": [FakeColumn("email", "text")],
        },
    )

    assert adapter.relation_calls == ["orders"]
    assert adapter.column_calls == ["orders"]


def test_include_and_exclude_filters_still_apply() -> None:
    orders = model_node("orders")
    users = model_node("users")
    tests = [
        make_test_node(
            "null_anomaly_orders_email",
            attached_node=orders.unique_id,
            column_name="email",
        ),
        make_test_node(
            "null_anomaly_users_email",
            attached_node=users.unique_id,
            column_name="email",
        ),
    ]
    columns = {
        "orders": [FakeColumn("email", "text")],
        "users": [FakeColumn("email", "text")],
    }

    _, include_adapter = render_model(
        [orders, users, *tests], columns, include_only=["orders"]
    )
    _, exclude_adapter = render_model(
        [orders, users, *tests], columns, exclude_models=["orders"]
    )

    assert include_adapter.relation_calls == ["orders"]
    assert exclude_adapter.relation_calls == ["users"]


def test_empty_result_sql_is_preserved_when_no_columns_need_profiling() -> None:
    orders = model_node("orders")

    rendered, adapter = render_model(
        [orders], {"orders": [FakeColumn("id", "integer")]}
    )

    assert adapter.relation_calls == []
    assert "cast(null as varchar) as table_name" in rendered
    assert "where 1 = 0" in rendered
