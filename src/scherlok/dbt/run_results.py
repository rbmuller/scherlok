"""Read dbt's target/run_results.json execution artifact."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_run_results(project_dir: str | Path) -> dict[str, Any]:
    """Load and validate target/run_results.json from a dbt project."""
    run_results_path = Path(project_dir) / "target" / "run_results.json"
    if not run_results_path.is_file():
        raise FileNotFoundError(
            f"run_results.json not found at {run_results_path}. "
            f"Run `dbt run` again."
        )

    try:
        with run_results_path.open("r", encoding="utf-8") as f:
            run_results = json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(
            f"Malformed run_results.json at {run_results_path}: {exc}."
        ) from exc

    _validated_results(run_results)
    return run_results


def successful_model_unique_ids(run_results: object) -> set[str]:
    """Return unique IDs for successfully executed dbt model nodes."""
    return successful_model_unique_ids_from_results(_validated_results(run_results))


def successful_model_unique_ids_from_results(
    results: list[dict[str, Any]],
) -> set[str]:
    """Return successful model IDs from an already-validated results list."""
    return {
        result["unique_id"]
        for result in results
        if result["status"] == "success"
        and result["unique_id"].startswith("model.")
    }


def _validated_results(run_results: object) -> list[dict[str, Any]]:
    """Validate the artifact shape needed for execution filtering."""
    if not isinstance(run_results, dict):
        raise ValueError("Invalid run_results.json: top-level value must be an object.")

    results = run_results.get("results")
    if not isinstance(results, list):
        raise ValueError("Invalid run_results.json: top-level `results` must be a list.")

    validated: list[dict[str, Any]] = []
    for index, result in enumerate(results):
        if not isinstance(result, dict):
            raise ValueError(
                f"Invalid run_results.json: `results[{index}]` must be an object."
            )
        unique_id = result.get("unique_id")
        status = result.get("status")
        if not isinstance(unique_id, str) or not unique_id:
            raise ValueError(
                f"Invalid run_results.json: `results[{index}].unique_id` must be a string."
            )
        if not isinstance(status, str) or not status:
            raise ValueError(
                f"Invalid run_results.json: `results[{index}].status` must be a string."
            )
        validated.append(result)

    return validated
