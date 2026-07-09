"""Claude-augmented anomaly explanations for `--explain`.

One API call per watch run: the whole anomaly set is batched into a single
prompt so cost stays bounded and the model can correlate across tables
(e.g. four anomalies all tracing back to one upstream source). Only
aggregate data ever leaves the machine — anomaly type/severity/message
strings, lineage node names, history timestamps. No warehouse rows, no
cell values, no credentials.

Opt-in and fail-open by design: any failure (missing key, missing package,
timeout, rate limit, malformed response) raises ExplainUnavailableError, which
callers turn into a one-line note on the unaugmented alert. The original
alert is never blocked.
"""

from __future__ import annotations

import json
import os
from typing import Any

DEFAULT_MODEL = "claude-haiku-4-5-20251001"
MODEL_ENV_VAR = "SCHERLOK_EXPLAIN_MODEL"
API_KEY_ENV_VAR = "ANTHROPIC_API_KEY"
AUTH_TOKEN_ENV_VAR = "ANTHROPIC_AUTH_TOKEN"

REQUEST_TIMEOUT_SECONDS = 30.0
MAX_OUTPUT_TOKENS = 1024
MAX_DIAGNOSTIC_STEPS = 3
MAX_HISTORY_ENTRIES = 20
MAX_LINEAGE_PARENTS = 10
HISTORY_LOOKBACK_DAYS = 7
_MAX_ERROR_CHARS = 200

EXPLANATION_TOOL_NAME = "record_explanation"
EXPLANATION_HEADER = "🧠 AI hypothesis (--explain)"
UNAVAILABLE_NOTE_TEMPLATE = "(--explain unavailable: {error})"

# A forced tool call is the structured-output mechanism that works on every
# anthropic SDK >= 0.40 and on any model the user overrides to; the newer
# output_config/response_format parameters would pin us to recent SDKs.
_EXPLANATION_TOOL: dict[str, Any] = {
    "name": EXPLANATION_TOOL_NAME,
    "description": (
        "Record the root-cause hypothesis for this batch of data-quality anomalies."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "summary": {
                "type": "string",
                "description": (
                    "At most two sentences correlating the anomalies in the batch."
                ),
            },
            "likely_cause": {
                "type": "string",
                "description": "The single most likely root cause, stated plainly.",
            },
            "diagnostic_steps": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Up to three concrete next checks, most informative first."
                ),
            },
        },
        "required": ["summary", "likely_cause", "diagnostic_steps"],
    },
}

_SYSTEM_PROMPT = """\
You are Scherlok's on-call assistant. Scherlok is a zero-config data-quality \
monitor: it learns per-table baselines (row counts, schema, freshness cadence, \
NULL rates, value distributions, cardinality) and alerts when the current state \
deviates. You receive one JSON bundle per watch run containing every anomaly \
that fired, plus optional dbt lineage (table -> upstream parents) and the \
per-table anomaly history of the last 7 days.

Detector catalog — what each anomaly type means:
- volume_drop: row count fell sharply versus the learned baseline.
- volume_spike: row count far above the learned baseline.
- table_empty: the table has zero rows.
- freshness_critical / freshness_stale: the table has not been updated within \
its learned update cadence.
- column_removed / column_added / type_changed: schema drift versus the \
baseline schema.
- null_rate_change: a column's NULL rate shifted materially versus baseline.
- distribution_shift: a numeric column's mean moved several standard \
deviations from baseline.
- cardinality_change: a column's distinct-value count changed sharply \
(e.g. an enum column exploded).

Your job: give the on-call engineer a head start, not a lecture.
- Correlate across the batch. Multiple anomalies frequently share one \
upstream cause; use lineage (parents break before children) and recent \
history (recurring vs first-time) to pick it.
- Prefer the most upstream plausible cause over per-table symptoms.
- diagnostic_steps must be concrete and runnable: a SQL probe, a scherlok \
command (investigate/history), a dbt or migration check — not "investigate \
further".
- Be decisive. State the most likely hypothesis; do not enumerate every \
possibility. If the evidence is thin, say what single fact would confirm it.
- Record your answer with the record_explanation tool: summary of at most \
two sentences, one likely_cause, at most three diagnostic_steps."""


class ExplainUnavailableError(Exception):
    """The explanation could not be produced. The message is operator-facing
    and safe to append to the alert as `(--explain unavailable: <message>)`."""


def build_bundle(
    anomalies: list[dict],
    *,
    recent_history: list[dict] | None = None,
    lineage: dict[str, list[str]] | None = None,
    meta: dict | None = None,
) -> dict:
    """Build the aggregate-only bundle sent to the API.

    Strips anomalies down to {table, type, severity, message} and caps
    history/lineage sizes so the prompt stays small no matter how noisy
    the run was. Severity enums serialize as their bare name.
    """
    bundle: dict[str, Any] = {
        "anomalies": [
            {
                "table": a.get("table"),
                "type": a.get("type"),
                "severity": _severity_name(a.get("severity")),
                "message": a.get("message"),
            }
            for a in anomalies
        ]
    }
    if lineage:
        trimmed = {
            table: parents[:MAX_LINEAGE_PARENTS]
            for table, parents in lineage.items()
            if parents
        }
        if trimmed:
            bundle["lineage"] = trimmed
    if recent_history:
        bundle["recent_history"] = [
            {
                "table": h.get("table"),
                "type": h.get("type"),
                "severity": _severity_name(h.get("severity")),
                "detected_at": h.get("detected_at"),
            }
            for h in recent_history[:MAX_HISTORY_ENTRIES]
        ]
    if meta:
        bundle["meta"] = meta
    return bundle


def explain_anomalies(bundle: dict) -> dict | None:
    """One Anthropic call for the whole bundle.

    Returns {"summary", "likely_cause", "diagnostic_steps"} on success,
    None when the bundle has no anomalies (no API call is made), and
    raises ExplainUnavailableError on any failure.
    """
    if not bundle.get("anomalies"):
        return None

    client = _build_client()
    model = os.environ.get(MODEL_ENV_VAR) or DEFAULT_MODEL
    try:
        response = client.messages.create(
            model=model,
            max_tokens=MAX_OUTPUT_TOKENS,
            # The system prompt is the stable prefix shared by every run; the
            # breakpoint is harmless below the model's minimum cacheable size
            # and starts paying for itself if the catalog grows past it.
            system=[
                {
                    "type": "text",
                    "text": _SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            tools=[_EXPLANATION_TOOL],
            tool_choice={"type": "tool", "name": EXPLANATION_TOOL_NAME},
            messages=[{"role": "user", "content": _render_bundle(bundle)}],
        )
    except Exception as exc:
        raise ExplainUnavailableError(_describe_error(exc)) from exc
    return _parse_response(response)


def _build_client() -> Any:
    """Import anthropic lazily and construct a tight-timeout client.

    Lazy so that `pip install scherlok` without the [explain] extra never
    pays the import, and so the missing-package error surfaces as a clean
    ExplainUnavailableError instead of an ImportError at CLI startup.
    """
    if not (os.environ.get(API_KEY_ENV_VAR) or os.environ.get(AUTH_TOKEN_ENV_VAR)):
        raise ExplainUnavailableError(f"{API_KEY_ENV_VAR} is not set")
    try:
        import anthropic
    except ImportError as exc:
        raise ExplainUnavailableError(
            "anthropic package not installed — pip install 'scherlok[explain]'"
        ) from exc
    return anthropic.Anthropic(timeout=REQUEST_TIMEOUT_SECONDS, max_retries=1)


def format_explanation_text(explanation: dict) -> str:
    """Plain-text rendering shared by the Discord/Teams/email alert paths."""
    lines = [EXPLANATION_HEADER, str(explanation.get("summary") or "")]
    cause = explanation.get("likely_cause")
    if cause:
        lines.append(f"Likely cause: {cause}")
    steps = explanation.get("diagnostic_steps") or []
    if steps:
        lines.append("Check next:")
        lines.extend(f"  {i}. {step}" for i, step in enumerate(steps, 1))
    return "\n".join(line for line in lines if line)


def format_unavailable_note(error: str) -> str:
    """One-line note appended to alerts when the explanation failed."""
    return UNAVAILABLE_NOTE_TEMPLATE.format(error=error)


def _render_bundle(bundle: dict) -> str:
    return "Anomaly bundle for this watch run:\n" + json.dumps(
        bundle, sort_keys=True, default=str
    )


def _severity_name(severity: Any) -> str:
    """Serialize a Severity enum (or plain string) as its bare name."""
    return str(getattr(severity, "value", severity))


def _describe_error(exc: Exception) -> str:
    text = f"{type(exc).__name__}: {exc}".strip()
    if len(text) > _MAX_ERROR_CHARS:
        text = text[: _MAX_ERROR_CHARS - 1] + "…"
    return text


def _parse_response(response: Any) -> dict:
    """Extract and validate the forced tool call from the API response."""
    tool_use = next(
        (b for b in response.content if getattr(b, "type", None) == "tool_use"),
        None,
    )
    data = getattr(tool_use, "input", None)
    if not isinstance(data, dict):
        raise ExplainUnavailableError("model returned no structured explanation")

    summary = str(data.get("summary") or "").strip()
    likely_cause = str(data.get("likely_cause") or "").strip()
    steps = [
        str(s).strip()
        for s in (data.get("diagnostic_steps") or [])
        if str(s).strip()
    ][:MAX_DIAGNOSTIC_STEPS]
    if not summary or not likely_cause:
        raise ExplainUnavailableError("model returned an incomplete explanation")
    return {
        "summary": summary,
        "likely_cause": likely_cause,
        "diagnostic_steps": steps,
    }
