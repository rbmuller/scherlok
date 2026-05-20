"""Tests for `scherlok.dbt.lineage`."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scherlok.dbt.lineage import (
    build_dependency_graph,
    display_name,
    downstream_of,
    invert_graph,
    render_lineage_tree,
    upstream_of,
)

FIXTURE = (
    Path(__file__).parent
    / "fixtures" / "dbt" / "jaffle_shop_postgres" / "target" / "manifest.json"
)


@pytest.fixture
def manifest() -> dict:
    return json.loads(FIXTURE.read_text())


@pytest.fixture
def graph(manifest: dict) -> dict[str, list[str]]:
    return build_dependency_graph(manifest)


# ----- build_dependency_graph ----------------------------------------------

def test_build_graph_returns_dict_of_lists(graph):
    assert isinstance(graph, dict)
    assert all(isinstance(v, list) for v in graph.values())


def test_build_graph_is_defensive_copy(manifest):
    g = build_dependency_graph(manifest)
    g["model.jaffle_shop.fct_orders"].append("polluted")
    g2 = build_dependency_graph(manifest)
    assert "polluted" not in g2["model.jaffle_shop.fct_orders"]


def test_build_graph_empty_manifest_returns_empty_dict():
    assert build_dependency_graph({}) == {}


def test_build_graph_missing_parent_map_returns_empty_dict():
    assert build_dependency_graph({"nodes": {}, "metadata": {}}) == {}


# ----- upstream_of ----------------------------------------------------------

def test_upstream_of_leaf_returns_empty(graph):
    # seeds have no upstream by construction in our fixture
    assert upstream_of(graph, "seed.jaffle_shop.raw_customers") == []


def test_upstream_of_fct_orders_walks_full_ancestry(graph):
    ancestors = upstream_of(graph, "model.jaffle_shop.fct_orders")
    # fct_orders depends on int_orders_pivoted + stg_customers transitively
    # reaching stg_orders, raw payments source, and raw_customers seed.
    assert "model.jaffle_shop.int_orders_pivoted" in ancestors
    assert "model.jaffle_shop.stg_customers" in ancestors
    assert "model.jaffle_shop.stg_orders" in ancestors
    assert "source.jaffle_shop.raw.payments" in ancestors
    assert "seed.jaffle_shop.raw_customers" in ancestors
    # fct_orders itself must NOT be in its own upstream list
    assert "model.jaffle_shop.fct_orders" not in ancestors


def test_upstream_of_unknown_node_returns_empty(graph):
    assert upstream_of(graph, "model.does_not_exist") == []


def test_upstream_of_dedupes_diamond():
    """A true diamond (A inherits from B + C, both from D) emits D exactly once."""
    diamond = {
        "A": ["B", "C"],
        "B": ["D"],
        "C": ["D"],
        "D": [],
    }
    ancestors = upstream_of(diamond, "A")
    assert ancestors.count("D") == 1
    assert set(ancestors) == {"B", "C", "D"}


def test_upstream_of_excludes_self_under_cycle():
    """Even with A → B → A, walking from A must not emit A."""
    cyclic = {"A": ["B"], "B": ["A"]}
    assert "A" not in upstream_of(cyclic, "A")
    assert upstream_of(cyclic, "A") == ["B"]


def test_downstream_of_excludes_self_under_cycle():
    """Mirror of the upstream cycle guard, walking children."""
    cyclic = {"A": ["B"], "B": ["A"]}
    assert "A" not in downstream_of(cyclic, "A")
    assert downstream_of(cyclic, "A") == ["B"]


# ----- downstream_of --------------------------------------------------------

def test_downstream_of_root_walks_full_descent(graph):
    desc = downstream_of(graph, "seed.jaffle_shop.raw_customers")
    # raw_customers -> stg_customers -> {fct_orders, dim_customers_inc, test}
    assert "model.jaffle_shop.stg_customers" in desc
    assert "model.jaffle_shop.fct_orders" in desc
    assert "model.jaffle_shop.dim_customers_inc" in desc


def test_downstream_of_leaf_returns_empty(graph):
    assert downstream_of(graph, "model.jaffle_shop.fct_orders") == []


def test_downstream_of_unknown_node_returns_empty(graph):
    assert downstream_of(graph, "model.does_not_exist") == []


# ----- invert_graph ---------------------------------------------------------

def test_invert_graph_flips_edges(graph):
    children = invert_graph(graph)
    # raw_customers should appear as a child of nothing but as a parent of
    # stg_customers in the inverted view
    assert "model.jaffle_shop.stg_customers" in children["seed.jaffle_shop.raw_customers"]


def test_invert_graph_idempotent_via_double_invert(graph):
    twice = invert_graph(invert_graph(graph))
    # Twice-inverted graph holds the same edges as the original (per-node order
    # may differ); compare as sets per node.
    common = set(twice) & set(graph)
    for n in common:
        if graph[n]:  # only nodes that originally had parents
            assert set(twice[n]) == set(graph[n]), n


# ----- display_name --------------------------------------------------------

def test_display_name_strips_model_prefix():
    assert display_name("model.jaffle_shop.fct_orders") == "fct_orders"


def test_display_name_strips_source_prefix_keeping_source_name():
    assert display_name("source.jaffle_shop.raw.payments") == "raw.payments"


def test_display_name_passthrough_when_unrecognized():
    assert display_name("foo") == "foo"


# ----- render_lineage_tree -------------------------------------------------

def test_render_lineage_tree_renders_both_directions_for_middle_node(graph):
    tree = render_lineage_tree(graph, "model.jaffle_shop.stg_customers")
    assert "Upstream of stg_customers:" in tree
    assert "Downstream of stg_customers:" in tree
    assert "raw_customers" in tree  # upstream
    assert "fct_orders" in tree  # downstream
    assert "dim_customers_inc" in tree  # downstream


def test_render_lineage_tree_uses_box_drawing_glyphs(graph):
    tree = render_lineage_tree(graph, "model.jaffle_shop.stg_customers")
    assert "├──" in tree or "└──" in tree


def test_render_lineage_tree_omits_upstream_section_for_leaf(graph):
    tree = render_lineage_tree(graph, "seed.jaffle_shop.raw_customers")
    assert "Upstream of" not in tree
    assert "Downstream of raw_customers:" in tree


def test_render_lineage_tree_omits_downstream_section_for_sink(graph):
    tree = render_lineage_tree(graph, "model.jaffle_shop.fct_orders")
    assert "Upstream of fct_orders:" in tree
    assert "Downstream of" not in tree


def test_render_lineage_tree_max_depth_caps_recursion(graph):
    shallow = render_lineage_tree(graph, "seed.jaffle_shop.raw_customers", max_depth=1)
    deep = render_lineage_tree(graph, "seed.jaffle_shop.raw_customers", max_depth=10)
    assert len(deep) > len(shallow)
    # At depth=1 from raw_customers we only see stg_customers; deeper nodes
    # like fct_orders should not appear yet.
    assert "fct_orders" not in shallow
    assert "fct_orders" in deep
