"""Tests for adaptive baselines and detector integration."""

from math import inf, nan

import pytest

from scherlok.detector.adaptive import adaptive_baseline
from scherlok.detector.anomaly import detect_volume_anomalies
from scherlok.detector.cardinality import detect_cardinality_anomalies
from scherlok.detector.distribution_shift import detect_distribution_shift
from scherlok.detector.nullability import detect_nullability_anomalies
from scherlok.detector.severity import Severity


def _profiles(metric: str, values: list[object]) -> list[dict]:
    return [{metric: value} for value in values]


class TestAdaptiveBaseline:
    def test_falls_back_with_too_few_valid_values(self):
        history = _profiles("value", [1, 2, 3, 4, None, "5"])

        assert adaptive_baseline(history, "value") is None

    def test_calculates_median_and_scaled_mad(self):
        baseline = adaptive_baseline(_profiles("value", [1, 2, 3, 4, 5]), "value")

        assert baseline is not None
        assert baseline.center == 3
        assert baseline.scale == pytest.approx(1 / 0.6745)

    def test_resists_a_historical_outlier(self):
        baseline = adaptive_baseline(
            _profiles("value", [9, 9, 10, 10, 10, 11, 1000]), "value"
        )

        assert baseline is not None
        assert baseline.center == 10
        assert baseline.scale == pytest.approx(1 / 0.6745)

    def test_ignores_missing_non_numeric_and_non_finite_values(self):
        history = _profiles("value", [None, "bad", nan, inf, 1, 2, 3, 4, 5])

        baseline = adaptive_baseline(history, "value")

        assert baseline is not None
        assert baseline.center == 3

    def test_falls_back_for_zero_mad(self):
        assert adaptive_baseline(_profiles("value", [10, 10, 10, 10, 10]), "value") is None

    def test_uses_only_the_latest_thirty_profiles(self):
        values = [100] * 30 + [1, 2, 3, 4, 5]

        baseline = adaptive_baseline(_profiles("value", values), "value")

        assert baseline is None


class TestAdaptiveDetectors:
    def test_noisy_history_widens_volume_threshold(self):
        current = {"row_count": 250}
        stored = {"row_count": 100}
        history = _profiles("row_count", [50, 100, 150, 100, 150, 50])

        assert detect_volume_anomalies("t", current, stored)
        assert detect_volume_anomalies("t", current, stored, history=history) == []

    def test_small_statistically_unusual_volume_change_is_info(self):
        history = _profiles("row_count", [98, 99, 100, 101, 102, 100])

        anomalies = detect_volume_anomalies(
            "t", {"row_count": 105}, {"row_count": 100}, history=history
        )

        assert len(anomalies) == 1
        assert anomalies[0]["severity"] == Severity.INFO
        assert "learned baseline" in anomalies[0]["message"]

    @pytest.mark.parametrize(
        ("current_count", "severity"),
        [(250, Severity.WARNING), (500, Severity.CRITICAL)],
    )
    def test_volume_effect_size_severity_is_preserved(self, current_count, severity):
        history = _profiles("row_count", [98, 99, 100, 101, 102, 100])

        anomalies = detect_volume_anomalies(
            "t", {"row_count": current_count}, {"row_count": 100}, history=history
        )

        assert anomalies[0]["severity"] == severity

    def test_adaptive_distribution_uses_robust_sigma_and_never_critical(self):
        history = _profiles("mean", [98, 99, 100, 101, 102, 100])

        info = detect_distribution_shift(
            "t", "col", {"mean": 105, "stddev": 0}, {"mean": 100, "stddev": 0},
            history=history,
        )
        warning = detect_distribution_shift(
            "t", "col", {"mean": 110, "stddev": 0}, {"mean": 100, "stddev": 0},
            history=history,
        )

        assert info[0]["severity"] == Severity.INFO
        assert warning[0]["severity"] == Severity.WARNING
        assert warning[0]["severity"] != Severity.CRITICAL

    def test_adaptive_nullability_preserves_effect_size_severity(self):
        history = _profiles("null_rate", [0.01, 0.02, 0.03, 0.02, 0.01, 0.02])

        anomalies = detect_nullability_anomalies(
            "t", "col", {"null_rate": 0.15}, {"null_rate": 0.02}, history=history
        )

        assert anomalies[0]["severity"] == Severity.WARNING

    def test_adaptive_cardinality_preserves_effect_size_severity(self):
        history = _profiles("distinct_count", [98, 99, 100, 101, 102, 100])

        anomalies = detect_cardinality_anomalies(
            "t", "col", {"distinct_count": 500}, {"distinct_count": 100}, history=history
        )

        assert anomalies[0]["severity"] == Severity.CRITICAL

    def test_table_empty_remains_critical(self):
        history = _profiles("row_count", [98, 99, 100, 101, 102, 100])

        anomalies = detect_volume_anomalies(
            "t", {"row_count": 0}, {"row_count": 100}, history=history
        )

        empty = [anomaly for anomaly in anomalies if anomaly["type"] == "table_empty"]
        assert len(empty) == 1
        assert empty[0]["severity"] == Severity.CRITICAL

    def test_insufficient_history_preserves_legacy_behavior(self):
        current = {"row_count": 250}
        stored = {"row_count": 100}
        history = _profiles("row_count", [98, 99, 100, 101])

        assert detect_volume_anomalies("t", current, stored, history=history) == (
            detect_volume_anomalies("t", current, stored)
        )
