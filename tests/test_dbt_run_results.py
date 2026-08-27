"""Tests for dbt run_results.json parsing."""

import json
from pathlib import Path

import pytest

from scherlok.dbt import successful_model_unique_ids_from_results
from scherlok.dbt.run_results import load_run_results, successful_model_unique_ids


def _write_run_results(project_dir: Path, value: object) -> Path:
    target_dir = project_dir / "target"
    target_dir.mkdir()
    path = target_dir / "run_results.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def test_load_run_results_valid_artifact(tmp_path):
    artifact = {"metadata": {"dbt_version": "1.8.0"}, "results": []}
    _write_run_results(tmp_path, artifact)

    assert load_run_results(tmp_path) == artifact


def test_load_run_results_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError, match="run_results.json not found"):
        load_run_results(tmp_path)


def test_load_run_results_malformed_json(tmp_path):
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "run_results.json").write_text("{not json", encoding="utf-8")

    with pytest.raises(ValueError, match="Malformed run_results.json"):
        load_run_results(tmp_path)


def test_successful_model_unique_ids_extracts_only_successful_models():
    run_results = {
        "results": [
            {"unique_id": "model.demo.orders", "status": "success"},
            {"unique_id": "model.demo.customers", "status": "error"},
            {"unique_id": "model.demo.payments", "status": "skipped"},
            {"unique_id": "model.demo.products", "status": "partial success"},
            {"unique_id": "model.demo.inventory", "status": "no-op"},
            {"unique_id": "seed.demo.raw_orders", "status": "success"},
            {"unique_id": "test.demo.orders", "status": "success"},
            {"unique_id": "snapshot.demo.orders", "status": "success"},
        ]
    }

    assert successful_model_unique_ids(run_results) == {"model.demo.orders"}


def test_successful_model_unique_ids_from_results_filters_validated_rows():
    results = [
        {"unique_id": "model.demo.orders", "status": "success"},
        {"unique_id": "model.demo.customers", "status": "error"},
        {"unique_id": "model.demo.payments", "status": "skipped"},
        {"unique_id": "seed.demo.raw_orders", "status": "success"},
        {"unique_id": "test.demo.orders", "status": "success"},
        {"unique_id": "snapshot.demo.orders", "status": "success"},
    ]

    assert successful_model_unique_ids_from_results(results) == {"model.demo.orders"}


@pytest.mark.parametrize(
    "artifact",
    [
        [],
        {"metadata": {}},
        {"results": {}},
        {"results": ["not an object"]},
        {"results": [{"status": "success"}]},
        {"results": [{"unique_id": "model.demo.orders"}]},
    ],
)
def test_successful_model_unique_ids_rejects_unusable_structure(artifact):
    with pytest.raises(ValueError, match="Invalid run_results.json"):
        successful_model_unique_ids(artifact)
