"""CI/CD exit code determination based on anomaly severity."""

from scherlok.detector.severity import Severity


def exit_code_for(anomalies: list[dict]) -> int:
    """Return exit code 1 if any CRITICAL anomaly found, 0 otherwise."""
    for anomaly in anomalies:
        if anomaly.get("severity") == Severity.CRITICAL:
            return 1
    return 0
