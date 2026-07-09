"""Claude-augmented alert explanations — the engine behind `--explain`."""

from scherlok.explainer.engine import (
    DEFAULT_MODEL,
    EXPLANATION_HEADER,
    HISTORY_LOOKBACK_DAYS,
    MODEL_ENV_VAR,
    ExplainUnavailableError,
    build_bundle,
    explain_anomalies,
    format_explanation_text,
    format_unavailable_note,
)

__all__ = [
    "DEFAULT_MODEL",
    "EXPLANATION_HEADER",
    "HISTORY_LOOKBACK_DAYS",
    "MODEL_ENV_VAR",
    "ExplainUnavailableError",
    "build_bundle",
    "explain_anomalies",
    "format_explanation_text",
    "format_unavailable_note",
]
