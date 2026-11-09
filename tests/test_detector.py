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

    def test_distribution_shift_negative_z_symmetric(self):
        assert classify_distribution_shift(-6.0) == Severity.WARNING
        assert classify_distribution_shift(-3.5) == Severity.INFO

    def test_distribution_shift_never_critical(self):
        """A mean shift alone must not gate CI — pinned by design."""
        assert classify_distribution_shift(50.0) != Severity.CRITICAL


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

    def test_a_column_based_age_names_the_column(self):
        current = {"hours_since_update": 48.0, "freshness_source": "column",
                   "freshness_column": "updated_at"}
        stored = {"hours_since_update": 6.0, "freshness_source": "column",
                  "freshness_column": "updated_at"}
        anomalies = detect_freshness_anomalies("t", current, stored)
        assert len(anomalies) == 1
        assert anomalies[0]["column"] == "updated_at"
        assert "based on updated_at" in anomalies[0]["message"]

    def test_ages_from_different_sources_are_not_compared(self):
        """#139: the first run after an upgrade reads a column where the stored
        age came from metadata; a vacuum time against a load time is no signal."""
        current = {"hours_since_update": 20.0, "freshness_source": "column",
                   "freshness_column": "updated_at"}
        stored = {"hours_since_update": 1.5}
        assert detect_freshness_anomalies("t", current, stored) == []

    def test_a_stored_age_without_a_source_counts_as_metadata(self):
        current = {"hours_since_update": 48.0, "freshness_source": "metadata",
                   "freshness_column": None}
        stored = {"hours_since_update": 6.0}
        anomalies = detect_freshness_anomalies("t", current, stored)
        assert len(anomalies) == 1
        assert anomalies[0]["column"] is None


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

    def test_large_decrease_is_capped_at_warning(self):
        """#136: a fixed all-NULL column must not fail every run as CRITICAL."""
        current = {"null_rate": 0.0}
        stored = {"null_rate": 1.0}
        anomalies = detect_nullability_anomalies("t", "col", current, stored)
        assert len(anomalies) == 1
        assert anomalies[0]["severity"] == Severity.WARNING

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

    def test_no_anomaly_below_the_minimum_relative_change(self):
        """#138: a z-score of +50 on a 0.5% move is not a shift worth reporting."""
        current = {"mean": 100.5, "stddev": 0.01}
        stored = {"mean": 100.0, "stddev": 0.01}
        anomalies = detect_distribution_shift("t", "col", current, stored)
        assert anomalies == []

    def test_a_move_at_the_minimum_relative_change_is_reported(self):
        current = {"mean": 101.0, "stddev": 0.01}
        stored = {"mean": 100.0, "stddev": 0.01}
        anomalies = detect_distribution_shift("t", "col", current, stored)
        assert len(anomalies) == 1

    def test_minimum_relative_change_does_not_apply_to_a_zero_baseline(self):
        current = {"mean": 5.0, "stddev": 1.0}
        stored = {"mean": 0.0, "stddev": 1.0}
        anomalies = detect_distribution_shift("t", "col", current, stored)
        assert len(anomalies) == 1

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


class TestCardinalityNearUniqueColumns:
    """Use distinct ratio for columns that were historically near-unique."""

    STORED_DIST = {"distinct_count": 1000}
    STORED_VOL = {"row_count": 1000}

    def test_volume_drop_on_unique_column_is_silent(self):
        anomalies = detect_cardinality_anomalies(
            "orders",
            "id",
            {"distinct_count": 400},
            self.STORED_DIST,
            {"row_count": 400},
            self.STORED_VOL,
        )
        assert anomalies == []

    def test_duplicates_on_unique_column_still_fire(self):
        # An exact 50% uniqueness loss is CRITICAL (>= on both bands,
        # matching the absolute path).
        anomalies = detect_cardinality_anomalies(
            "orders",
            "id",
            {"distinct_count": 500},
            self.STORED_DIST,
            {"row_count": 1000},
            self.STORED_VOL,
        )
        assert len(anomalies) == 1
        assert anomalies[0]["severity"] == Severity.CRITICAL
        assert "decreased" in anomalies[0]["message"]
        assert "distinct ratio" in anomalies[0]["message"]

    def test_exact_warning_boundary_fires_despite_float_repr(self):
        # 1 - 0.9 is 0.09999999999999998 in binary floats; a clean 10%
        # loss must still warn.
        anomalies = detect_cardinality_anomalies(
            "orders",
            "id",
            {"distinct_count": 900},
            self.STORED_DIST,
            {"row_count": 1000},
            self.STORED_VOL,
        )
        assert len(anomalies) == 1
        assert anomalies[0]["severity"] == Severity.WARNING
        assert "distinct ratio" in anomalies[0]["message"]

    def test_partial_loss_below_critical_stays_warning(self):
        # 1000 -> 800 is a 20% uniqueness loss: clearly warning, and clear
        # of the 10% float edge.
        anomalies = detect_cardinality_anomalies(
            "orders",
            "id",
            {"distinct_count": 800},
            self.STORED_DIST,
            {"row_count": 1000},
            self.STORED_VOL,
        )
        assert len(anomalies) == 1
        assert anomalies[0]["severity"] == Severity.WARNING
        assert "distinct ratio" in anomalies[0]["message"]

    def test_nulling_unique_values_is_silent(self):
        # 20% of a unique e-mail column nulled: 1,600 remaining values are
        # still distinct, so the non-null ratio is unchanged and nothing fires.
        anomalies = detect_cardinality_anomalies(
            "users",
            "email",
            {"distinct_count": 1600, "null_count": 400},
            {"distinct_count": 2000, "null_count": 0},
            {"row_count": 2000},
            {"row_count": 2000},
        )
        assert anomalies == []

    def test_null_aware_ratio_still_fires_on_real_duplicates(self):
        # Same 20% NULLs, but only 800 distinct among the 1,600 non-null:
        # uniqueness halved, so it must fire.
        anomalies = detect_cardinality_anomalies(
            "users",
            "email",
            {"distinct_count": 800, "null_count": 400},
            {"distinct_count": 2000, "null_count": 0},
            {"row_count": 2000},
            {"row_count": 2000},
        )
        assert len(anomalies) == 1
        assert anomalies[0]["severity"] == Severity.CRITICAL
        assert "distinct ratio" in anomalies[0]["message"]

    def test_low_cardinality_column_keeps_absolute_comparison(self):
        anomalies = detect_cardinality_anomalies(
            "users",
            "plan",
            {"distinct_count": 605},
            {"distinct_count": 5},
            {"row_count": 400},
            {"row_count": 1000},
        )
        assert len(anomalies) == 1
        assert anomalies[0]["severity"] == Severity.CRITICAL
        assert "distinct values" in anomalies[0]["message"]

    def test_falls_back_to_absolute_without_volume_profiles(self):
        anomalies = detect_cardinality_anomalies(
            "orders", "id", {"distinct_count": 400}, self.STORED_DIST
        )
        assert len(anomalies) == 1
        assert anomalies[0]["severity"] == Severity.WARNING

    def test_falls_back_to_absolute_on_zero_row_counts(self):
        anomalies = detect_cardinality_anomalies(
            "orders",
            "id",
            {"distinct_count": 400},
            self.STORED_DIST,
            {"row_count": 0},
            {"row_count": 0},
        )
        assert len(anomalies) == 1
        assert anomalies[0]["severity"] == Severity.WARNING

    def test_column_just_below_unique_threshold_uses_absolute(self):
        anomalies = detect_cardinality_anomalies(
            "orders",
            "customer_id",
            {"distinct_count": 360},
            {"distinct_count": 900},
            {"row_count": 400},
            {"row_count": 1000},
        )
        assert len(anomalies) == 1
        assert "distinct values" in anomalies[0]["message"]

    def test_large_loss_of_uniqueness_is_critical(self):
        anomalies = detect_cardinality_anomalies(
            "orders",
            "id",
            {"distinct_count": 100},
            self.STORED_DIST,
            {"row_count": 1000},
            self.STORED_VOL,
        )
        assert len(anomalies) == 1
        assert anomalies[0]["severity"] == Severity.CRITICAL
        assert "distinct ratio" in anomalies[0]["message"]


class TestCardinalityOnASample:
    """`sampled=True` (`verify --ignore volume`): only what a sample can't explain fires."""

    STORED_VOL = {"row_count": 10_000}
    SAMPLE_VOL = {"row_count": 1_000}
    # A steady learned baseline of 2,000 customers, as a production table has.
    HISTORY = [{"distinct_count": count} for count in (1990, 1995, 2000, 2005, 2010)]

    def _detect(self, current: int, stored: int, **kwargs):
        return detect_cardinality_anomalies(
            "orders",
            "customer_id",
            {"distinct_count": current},
            {"distinct_count": stored},
            self.SAMPLE_VOL,
            self.STORED_VOL,
            sampled=True,
            **kwargs,
        )

    def test_fewer_distinct_values_in_a_sample_are_silent(self):
        assert self._detect(current=600, stored=2000) == []
        assert self._detect(current=1, stored=50) == []  # one tenant's slice

    def test_the_absolute_comparison_still_fires_without_sampled(self):
        """Control: the same decrease fires when the table is not a sample."""
        anomalies = detect_cardinality_anomalies(
            "orders", "customer_id", {"distinct_count": 600}, {"distinct_count": 2000},
            self.SAMPLE_VOL, self.STORED_VOL,
        )
        assert [a["severity"] for a in anomalies] == [Severity.WARNING]

    def test_more_distinct_values_than_the_baseline_still_fire(self):
        anomalies = self._detect(current=6000, stored=2000)

        assert len(anomalies) == 1
        assert anomalies[0]["severity"] == Severity.CRITICAL
        assert "increased" in anomalies[0]["message"]

    def test_lost_uniqueness_still_fires_on_a_sample(self):
        anomalies = detect_cardinality_anomalies(
            "orders", "id", {"distinct_count": 400}, {"distinct_count": 10_000},
            self.SAMPLE_VOL, self.STORED_VOL, sampled=True,
        )

        assert [a["severity"] for a in anomalies] == [Severity.CRITICAL]
        assert "distinct ratio" in anomalies[0]["message"]

    def test_the_learned_baseline_is_not_used(self):
        learned = self._detect(current=600, stored=2000, history=self.HISTORY)
        unsampled = detect_cardinality_anomalies(
            "orders", "customer_id", {"distinct_count": 600}, {"distinct_count": 2000},
            self.SAMPLE_VOL, self.STORED_VOL, history=self.HISTORY,
        )

        assert learned == []
        assert "learned baseline" in unsampled[0]["message"]
