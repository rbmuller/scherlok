"""Tests for anomaly detection modules."""

from scherlok.detector.anomaly import detect_volume_anomalies, z_score
from scherlok.detector.cardinality import detect_cardinality_anomalies
from scherlok.detector.distribution_shift import detect_distribution_shift
from scherlok.detector.freshness import detect_freshness_anomalies
from scherlok.detector.nullability import detect_nullability_anomalies
from scherlok.detector.schema_drift import detect_schema_drift
from scherlok.detector.severity import (
    Severity,
    classify_distribution_shift,
    classify_volume_drop,
)


class TestZScore:
    """Tests for z-score calculation."""

    def test_basic_z_score(self):
        result = z_score(current=120, mean=100, stddev=10)
        assert result == 2.0

    def test_z_score_zero_stddev(self):
        result = z_score(current=100, mean=100, stddev=0)
        assert result is None

    def test_negative_z_score(self):
        result = z_score(current=80, mean=100, stddev=10)
        assert result == -2.0


class TestVolumeAnomalies:
    """Tests for volume anomaly detection."""

    def test_no_anomaly_when_stable(self):
        current = {"row_count": 1000, "timestamp": "2026-04-01T00:00:00"}
        stored = {"row_count": 1000, "timestamp": "2026-03-31T00:00:00"}
        anomalies = detect_volume_anomalies("t", current, stored)
        assert len(anomalies) == 0

    def test_warning_on_moderate_drop(self):
        current = {"row_count": 700, "timestamp": "2026-04-01T00:00:00"}
        stored = {"row_count": 1000, "timestamp": "2026-03-31T00:00:00"}
        anomalies = detect_volume_anomalies("t", current, stored)
        assert len(anomalies) == 1
        assert anomalies[0]["severity"] == Severity.WARNING

    def test_critical_on_large_drop(self):
        current = {"row_count": 400, "timestamp": "2026-04-01T00:00:00"}
        stored = {"row_count": 1000, "timestamp": "2026-03-31T00:00:00"}
        anomalies = detect_volume_anomalies("t", current, stored)
        assert any(a["severity"] == Severity.CRITICAL for a in anomalies)

    def test_critical_on_empty_table(self):
        current = {"row_count": 0, "timestamp": "2026-04-01T00:00:00"}
        stored = {"row_count": 1000, "timestamp": "2026-03-31T00:00:00"}
        anomalies = detect_volume_anomalies("t", current, stored)
        assert any(a["type"] == "table_empty" for a in anomalies)

    def test_no_anomaly_both_zero(self):
        current = {"row_count": 0, "timestamp": "2026-04-01T00:00:00"}
        stored = {"row_count": 0, "timestamp": "2026-03-31T00:00:00"}
        anomalies = detect_volume_anomalies("t", current, stored)
        assert len(anomalies) == 0

    def test_no_anomaly_on_small_growth(self):
        current = {"row_count": 1500, "timestamp": "2026-04-01T00:00:00"}
        stored = {"row_count": 1000, "timestamp": "2026-03-31T00:00:00"}
        anomalies = detect_volume_anomalies("t", current, stored)
        assert len(anomalies) == 0  # 50% growth is normal

    def test_warning_on_spike(self):
        current = {"row_count": 2500, "timestamp": "2026-04-01T00:00:00"}
        stored = {"row_count": 1000, "timestamp": "2026-03-31T00:00:00"}
        anomalies = detect_volume_anomalies("t", current, stored)
        assert len(anomalies) == 1
        assert anomalies[0]["type"] == "volume_spike"

    def test_critical_on_huge_spike(self):
        current = {"row_count": 5000, "timestamp": "2026-04-01T00:00:00"}
        stored = {"row_count": 1000, "timestamp": "2026-03-31T00:00:00"}
        anomalies = detect_volume_anomalies("t", current, stored)
        assert len(anomalies) == 1
        assert anomalies[0]["severity"].value == "CRITICAL"


class TestSchemaDrift:
    """Tests for schema drift detection."""

    def test_no_drift(self):
        schema = {"columns": [{"name": "id", "type": "integer", "nullable": False}]}
        anomalies = detect_schema_drift("t", schema, schema)
        assert len(anomalies) == 0

    def test_column_added(self):
        stored = {"columns": [{"name": "id", "type": "integer", "nullable": False}]}
        current = {
            "columns": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "email", "type": "text", "nullable": True},
            ]
        }
        anomalies = detect_schema_drift("t", current, stored)
        assert len(anomalies) == 1
        assert anomalies[0]["type"] == "column_added"
        assert anomalies[0]["severity"] == Severity.CRITICAL

    def test_column_removed(self):
        stored = {
            "columns": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "email", "type": "text", "nullable": True},
            ]
        }
        current = {"columns": [{"name": "id", "type": "integer", "nullable": False}]}
        anomalies = detect_schema_drift("t", current, stored)
        assert len(anomalies) == 1
        assert anomalies[0]["type"] == "column_removed"

    def test_type_changed(self):
        stored = {"columns": [{"name": "score", "type": "integer", "nullable": False}]}
        current = {"columns": [{"name": "score", "type": "numeric", "nullable": False}]}
        anomalies = detect_schema_drift("t", current, stored)
        assert len(anomalies) == 1
        assert anomalies[0]["type"] == "type_changed"


class TestSeverityClassification:
    """Tests for severity classification helpers."""

    def test_volume_drop_critical(self):
        assert classify_volume_drop(400, 1000) == Severity.CRITICAL

    def test_volume_drop_warning(self):
        assert classify_volume_drop(750, 1000) == Severity.WARNING

    def test_volume_drop_none(self):
        assert classify_volume_drop(950, 1000) is None

    def test_volume_drop_zero_previous(self):
        assert classify_volume_drop(100, 0) is None

    def test_distribution_shift_warning(self):
        assert classify_distribution_shift(6.0) == Severity.WARNING

    def test_distribution_shift_info(self):
        assert classify_distribution_shift(3.5) == Severity.INFO


class TestFreshnessDetector:
    """Tests for freshness anomaly detection."""

    def test_no_anomaly_when_fresh(self):
        current = {"hours_since_update": 2.0}
        stored = {"hours_since_update": 2.0}
        anomalies = detect_freshness_anomalies("t", current, stored)
        assert len(anomalies) == 0

    def test_warning_when_double_overdue(self):
        current = {"hours_since_update": 10.0}
        stored = {"hours_since_update": 4.0}
        anomalies = detect_freshness_anomalies("t", current, stored)
        assert len(anomalies) == 1
        assert anomalies[0]["severity"] == Severity.WARNING
        assert anomalies[0]["type"] == "freshness_stale"

    def test_critical_when_4x_overdue(self):
        current = {"hours_since_update": 48.0}
        stored = {"hours_since_update": 6.0}
        anomalies = detect_freshness_anomalies("t", current, stored)
        assert len(anomalies) == 1
        assert anomalies[0]["severity"] == Severity.CRITICAL

    def test_no_anomaly_when_hours_none(self):
        current = {"hours_since_update": None}
        stored = {"hours_since_update": 2.0}
        anomalies = detect_freshness_anomalies("t", current, stored)
        assert len(anomalies) == 0

    def test_no_anomaly_when_stored_too_small(self):
        current = {"hours_since_update": 0.5}
        stored = {"hours_since_update": 0.2}
        anomalies = detect_freshness_anomalies("t", current, stored)
        assert len(anomalies) == 0


class TestNullabilityDetector:
    """Tests for nullability anomaly detection."""

    def test_no_anomaly_when_stable(self):
        current = {"null_rate": 0.02}
        stored = {"null_rate": 0.02}
        anomalies = detect_nullability_anomalies("t", "col", current, stored)
        assert len(anomalies) == 0

    def test_warning_on_moderate_increase(self):
        current = {"null_rate": 0.15}
        stored = {"null_rate": 0.02}
        anomalies = detect_nullability_anomalies("t", "col", current, stored)
        assert len(anomalies) == 1
        assert anomalies[0]["severity"] == Severity.WARNING

    def test_critical_on_large_increase(self):
        current = {"null_rate": 0.45}
        stored = {"null_rate": 0.02}
        anomalies = detect_nullability_anomalies("t", "col", current, stored)
        assert len(anomalies) == 1
        assert anomalies[0]["severity"] == Severity.CRITICAL

    def test_detects_decrease(self):
        current = {"null_rate": 0.0}
        stored = {"null_rate": 0.35}
        anomalies = detect_nullability_anomalies("t", "col", current, stored)
        assert len(anomalies) == 1
        assert "decreased" in anomalies[0]["message"]

    def test_no_anomaly_when_none(self):
        current = {"null_rate": None}
        stored = {"null_rate": 0.02}
        anomalies = detect_nullability_anomalies("t", "col", current, stored)
        assert len(anomalies) == 0


class TestDistributionShiftDetector:
    """Tests for distribution shift detection."""

    def test_no_anomaly_when_stable(self):
        current = {"mean": 100.0, "stddev": 10.0}
        stored = {"mean": 100.0, "stddev": 10.0}
        anomalies = detect_distribution_shift("t", "col", current, stored)
        assert len(anomalies) == 0

    def test_info_on_moderate_shift(self):
        current = {"mean": 140.0, "stddev": 10.0}
        stored = {"mean": 100.0, "stddev": 10.0}
        anomalies = detect_distribution_shift("t", "col", current, stored)
        assert len(anomalies) == 1
        assert anomalies[0]["type"] == "distribution_shift"

    def test_warning_on_large_shift(self):
        current = {"mean": 170.0, "stddev": 10.0}
        stored = {"mean": 100.0, "stddev": 10.0}
        anomalies = detect_distribution_shift("t", "col", current, stored)
        assert len(anomalies) == 1
        assert anomalies[0]["severity"] == Severity.WARNING

    def test_no_anomaly_non_numeric(self):
        current = {"mean": None, "stddev": None}
        stored = {"mean": None, "stddev": None}
        anomalies = detect_distribution_shift("t", "col", current, stored)
        assert len(anomalies) == 0

    def test_no_anomaly_zero_stddev(self):
        current = {"mean": 105.0, "stddev": 0.0}
        stored = {"mean": 100.0, "stddev": 0.0}
        anomalies = detect_distribution_shift("t", "col", current, stored)
        assert len(anomalies) == 0


class TestCardinalityDetector:
    """Tests for cardinality change detection."""

    def test_no_anomaly_when_stable(self):
        current = {"distinct_count": 100}
        stored = {"distinct_count": 100}
        anomalies = detect_cardinality_anomalies("t", "col", current, stored)
        assert len(anomalies) == 0

    def test_warning_on_moderate_change(self):
        current = {"distinct_count": 160}
        stored = {"distinct_count": 100}
        anomalies = detect_cardinality_anomalies("t", "col", current, stored)
        assert len(anomalies) == 1
        assert anomalies[0]["severity"] == Severity.WARNING

    def test_critical_on_large_change(self):
        current = {"distinct_count": 500}
        stored = {"distinct_count": 100}
        anomalies = detect_cardinality_anomalies("t", "col", current, stored)
        assert len(anomalies) == 1
        assert anomalies[0]["severity"] == Severity.CRITICAL

    def test_detects_decrease(self):
        current = {"distinct_count": 2}
        stored = {"distinct_count": 100}
        anomalies = detect_cardinality_anomalies("t", "col", current, stored)
        assert len(anomalies) == 1
        assert "decreased" in anomalies[0]["message"]

    def test_no_anomaly_zero_stored(self):
        current = {"distinct_count": 10}
        stored = {"distinct_count": 0}
        anomalies = detect_cardinality_anomalies("t", "col", current, stored)
        assert len(anomalies) == 0
