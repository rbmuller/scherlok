"""CI/CD gate: decide pass/fail and the exit code from anomaly severity.

`--fail-on` is decided here and only here, so `ci`, `check`, `dbt`, `verify`
and the MCP `check`/`verify` tools cannot drift apart.
"""

from collections.abc import Iterable

from scherlok.detector.severity import Severity

FAIL_ON_CRITICAL = "critical"
FAIL_ON_WARNING = "warning"

_FAILING_SEVERITIES: dict[str, frozenset[Severity]] = {
    FAIL_ON_CRITICAL: frozenset({Severity.CRITICAL}),
    FAIL_ON_WARNING: frozenset({Severity.WARNING, Severity.CRITICAL}),
}
FAIL_ON_CHOICES = tuple(_FAILING_SEVERITIES)


def normalize_fail_on(fail_on: str) -> str:
    """Return the lower-cased threshold, or raise ValueError for an unknown one.

    Unknown values are rejected rather than treated as `critical`: a mistyped
    `warning` would otherwise loosen the gate without anyone noticing.
    """
    threshold = fail_on.lower()
    if threshold not in _FAILING_SEVERITIES:
        choices = " or ".join(f"'{choice}'" for choice in FAIL_ON_CHOICES)
        raise ValueError(f"fail_on must be {choices}, got '{fail_on}'")
    return threshold


def fails_gate(anomalies: Iterable[dict], fail_on: str = FAIL_ON_CRITICAL) -> bool:
    """True when any anomaly is at or above the `fail_on` threshold."""
    failing = _FAILING_SEVERITIES[normalize_fail_on(fail_on)]
    return any(anomaly.get("severity") in failing for anomaly in anomalies)


def exit_code_for(anomalies: list[dict]) -> int:
    """Return exit code 1 if any CRITICAL anomaly found, 0 otherwise."""
    return 1 if fails_gate(anomalies, FAIL_ON_CRITICAL) else 0
