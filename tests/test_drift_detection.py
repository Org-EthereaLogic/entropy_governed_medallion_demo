"""
Tests for Shannon Entropy drift detection.

These tests verify the core innovation: entropy-based drift detection
that catches distribution problems rule-based checks miss.

Author: Anthony Johnson | EthereaLogic LLC
"""

from entropy_governed_medallion.entropy.drift_detector import DriftDetector


class TestColumnDrift:
    """Test individual column drift detection."""

    def setup_method(self):
        self.detector = DriftDetector(
            entropy_drop_pct=0.50,
            entropy_spike_pct=0.50,
            health_score_floor=0.70,
        )

    def test_stable_column_no_drift(self):
        result = self.detector.detect_column_drift("status", 3.2, 3.1)
        assert not result.drift_detected
        assert result.drift_direction == "STABLE"
        assert result.stability_score > 0.9

    def test_entropy_collapse_detected(self):
        """Entropy drops by more than 50% — silent source failure."""
        result = self.detector.detect_column_drift("status", 3.2, 1.0)
        assert result.drift_detected
        assert result.drift_direction == "COLLAPSE"
        assert result.drift_magnitude > 0.5

    def test_entropy_spike_detected(self):
        """Entropy rises by more than 50% — new category injection."""
        result = self.detector.detect_column_drift("region", 2.0, 3.5)
        assert result.drift_detected
        assert result.drift_direction == "SPIKE"
        assert result.drift_magnitude > 0.5

    def test_constant_to_constant_stable(self):
        """Both zero — column was constant and remains constant."""
        result = self.detector.detect_column_drift("flag", 0.0, 0.0)
        assert not result.drift_detected
        assert result.stability_score == 1.0

    def test_constant_to_varied_spike(self):
        """Column was constant (H=0), now has variation."""
        result = self.detector.detect_column_drift("flag", 0.0, 2.5)
        assert result.drift_detected
        assert result.drift_direction == "SPIKE"
        assert result.stability_score == 0.0

    def test_exact_threshold_boundary(self):
        """Exactly at 50% drop — should NOT trigger (strict < comparison)."""
        # 3.0 -> 1.5 is exactly 50% drop
        result = self.detector.detect_column_drift("col", 3.0, 1.5)
        assert not result.drift_detected
        assert result.drift_direction == "STABLE"


class TestTableHealth:
    """Test composite table health scoring."""

    def setup_method(self):
        self.detector = DriftDetector(
            entropy_drop_pct=0.50,
            entropy_spike_pct=0.50,
            health_score_floor=0.70,
        )

    def test_healthy_table_passes_gate(self):
        baseline = [
            {"column_name": "id", "entropy": 10.0},
            {"column_name": "status", "entropy": 2.5},
            {"column_name": "amount", "entropy": 8.0},
        ]
        current = [
            {"column_name": "id", "entropy": 9.8},
            {"column_name": "status", "entropy": 2.4},
            {"column_name": "amount", "entropy": 7.9},
        ]
        result = self.detector.compute_table_health(baseline, current)
        assert result.passed_gate is True
        assert result.health_score > 0.90
        assert result.columns_drifted == 0

    def test_drifted_table_fails_gate(self):
        baseline = [
            {"column_name": "id", "entropy": 10.0},
            {"column_name": "status", "entropy": 2.5},
            {"column_name": "amount", "entropy": 8.0},
        ]
        current = [
            {"column_name": "id", "entropy": 10.0},
            {"column_name": "status", "entropy": 0.1},  # collapsed
            {"column_name": "amount", "entropy": 0.5},  # collapsed
        ]
        result = self.detector.compute_table_health(baseline, current)
        assert result.passed_gate is False
        assert result.health_score < 0.70
        assert result.columns_drifted == 2

    def test_single_column_collapse_partial_impact(self):
        baseline = [
            {"column_name": "a", "entropy": 5.0},
            {"column_name": "b", "entropy": 5.0},
            {"column_name": "c", "entropy": 5.0},
            {"column_name": "d", "entropy": 5.0},
        ]
        current = [
            {"column_name": "a", "entropy": 5.0},
            {"column_name": "b", "entropy": 5.0},
            {"column_name": "c", "entropy": 0.1},  # collapsed
            {"column_name": "d", "entropy": 5.0},
        ]
        result = self.detector.compute_table_health(baseline, current)
        # 3 of 4 columns stable = ~0.75 health, should still pass 0.70
        assert result.columns_drifted == 1
        assert result.health_score >= 0.70

    def test_empty_common_columns_fails(self):
        baseline = [{"column_name": "x", "entropy": 5.0}]
        current = [{"column_name": "y", "entropy": 5.0}]
        result = self.detector.compute_table_health(baseline, current)
        assert result.passed_gate is False
        assert result.health_score == 0.0

    def test_weighted_columns(self):
        """Business-critical columns can be weighted higher."""
        baseline = [
            {"column_name": "revenue", "entropy": 8.0},
            {"column_name": "notes", "entropy": 6.0},
        ]
        current = [
            {"column_name": "revenue", "entropy": 1.0},  # collapsed
            {"column_name": "notes", "entropy": 6.0},    # stable
        ]
        # With equal weights, health ~0.5. With revenue weighted 3x...
        result_weighted = self.detector.compute_table_health(
            baseline, current,
            column_weights={"revenue": 3.0, "notes": 1.0},
        )
        result_equal = self.detector.compute_table_health(baseline, current)
        # Weighted should be worse because the drifted column matters more
        assert result_weighted.health_score < result_equal.health_score


class TestCustomThresholds:
    """Test that threshold configuration works correctly."""

    def test_tight_thresholds_detect_small_drift(self):
        detector = DriftDetector(
            entropy_drop_pct=0.10,  # very sensitive
            entropy_spike_pct=0.10,
            health_score_floor=0.90,
        )
        result = detector.detect_column_drift("col", 5.0, 4.0)
        assert result.drift_detected  # 20% drop exceeds 10% threshold

    def test_loose_thresholds_ignore_moderate_drift(self):
        detector = DriftDetector(
            entropy_drop_pct=0.80,  # very tolerant
            entropy_spike_pct=0.80,
            health_score_floor=0.30,
        )
        result = detector.detect_column_drift("col", 5.0, 3.0)
        assert not result.drift_detected  # 40% drop within 80% tolerance
