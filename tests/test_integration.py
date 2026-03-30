"""
End-to-end integration tests for the entropy-governed medallion pipeline.

Loads sample CSVs, computes entropy, detects drift, evaluates gates,
and verifies the full pipeline produces correct structured results.

Author: Anthony Johnson | EthereaLogic LLC
"""

from pathlib import Path

from entropy_governed_medallion.config import load_gate_config
from entropy_governed_medallion.contracts import (
    FidelityResult,
    RunContext,
    RunStatus,
    TableHealthResult,
)
from entropy_governed_medallion.entropy.drift_detector import DriftDetector
from entropy_governed_medallion.gates.evaluator import evaluate_gates
from entropy_governed_medallion.provenance.builder import build_provenance_envelope
from entropy_governed_medallion.runners.local_demo import (
    compute_entropy_profile,
    load_csv,
    run_demo,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_DIR = PROJECT_ROOT / "data" / "sample"
CONFIG_PATH = PROJECT_ROOT / "config" / "kpi_thresholds.json"
EXCLUDE_COLUMNS = {"employee_id", "first_name", "last_name"}


class TestCSVLoading:
    """Verify sample data loads correctly."""

    def test_baseline_csv_loads(self):
        rows = load_csv(SAMPLE_DIR / "employees_sample.csv")
        assert len(rows) == 25
        assert "department" in rows[0]

    def test_drifted_csv_loads(self):
        rows = load_csv(SAMPLE_DIR / "employees_drifted.csv")
        assert len(rows) == 25

    def test_schemas_match(self):
        baseline = load_csv(SAMPLE_DIR / "employees_sample.csv")
        drifted = load_csv(SAMPLE_DIR / "employees_drifted.csv")
        assert set(baseline[0].keys()) == set(drifted[0].keys())


class TestEntropyProfileComputation:
    """Verify pure-Python entropy matches expected signals."""

    def test_baseline_has_diversity(self):
        rows = load_csv(SAMPLE_DIR / "employees_sample.csv")
        profile = compute_entropy_profile(rows, EXCLUDE_COLUMNS)
        dept = next(p for p in profile if p["column_name"] == "department")
        assert dept["distinct_count"] > 1
        assert dept["entropy"] > 0
        assert dept["entropy_class"] != "CONSTANT"

    def test_drifted_has_collapse(self):
        rows = load_csv(SAMPLE_DIR / "employees_drifted.csv")
        profile = compute_entropy_profile(rows, EXCLUDE_COLUMNS)
        dept = next(p for p in profile if p["column_name"] == "department")
        assert dept["distinct_count"] == 1
        assert dept["entropy"] == 0.0
        assert dept["entropy_class"] == "CONSTANT"

    def test_profile_covers_expected_columns(self):
        rows = load_csv(SAMPLE_DIR / "employees_sample.csv")
        profile = compute_entropy_profile(rows, EXCLUDE_COLUMNS)
        columns = {p["column_name"] for p in profile}
        assert "department" in columns
        assert "salary" in columns
        assert "location" in columns
        assert "employee_id" not in columns  # excluded


class TestEndToEndPipeline:
    """Full pipeline: CSV -> entropy -> drift -> gates -> provenance."""

    def test_drifted_data_fails_entropy_gate(self):
        """The core scenario: collapsed distributions block Gold refresh."""
        baseline_rows = load_csv(SAMPLE_DIR / "employees_sample.csv")
        current_rows = load_csv(SAMPLE_DIR / "employees_drifted.csv")

        baseline_profile = compute_entropy_profile(baseline_rows, EXCLUDE_COLUMNS)
        current_profile = compute_entropy_profile(current_rows, EXCLUDE_COLUMNS)

        detector = DriftDetector(
            entropy_drop_pct=0.50,
            entropy_spike_pct=0.50,
            health_score_floor=0.70,
        )
        health = detector.compute_table_health(baseline_profile, current_profile)

        # Health should be very low with 4 of 5 columns collapsed
        assert health.health_score < 0.70
        assert health.passed_gate is False
        assert health.columns_drifted >= 3

        # Gate evaluation should FAIL
        gate_config = load_gate_config(CONFIG_PATH)
        context = RunContext(
            experiment_id="TEST_INTEGRATION",
            run_id="integration_001",
            git_commit="abc123",
            git_branch="main",
            operator="pytest",
            started_at_utc="2026-03-30T00:00:00Z",
            dry_run=False,
        )
        health_contract = TableHealthResult(
            health_score=health.health_score,
            passed_gate=health.passed_gate,
            total_columns_checked=health.total_columns_checked,
            columns_drifted=health.columns_drifted,
            flagged_columns=health.flagged_columns,
            column_details=health.column_details,
        )
        fidelity = FidelityResult(
            source_row_count=25,
            target_row_count=25,
            row_count_ratio=1.0,
            columns_match=True,
        )
        provenance = build_provenance_envelope(
            context=context,
            status=RunStatus.EXECUTION_COMPLETED,
            completed_at_utc="2026-03-30T00:01:00Z",
            catalog_name="test",
            entropy_health=health_contract,
            tables_processed=("employees",),
        )
        gate_result = evaluate_gates(
            gate_config=gate_config,
            context=context,
            entropy_health=health_contract,
            fidelity=fidelity,
            provenance=provenance,
            quality_pass_ratio=1.0,
            silver_quarantine_ratio=0.0,
        )

        assert gate_result.overall_verdict == "FAIL"

        # Verify entropy gate specifically failed
        entropy_gate = next(
            e for e in gate_result.evaluations
            if e.metric == "entropy_health_score"
        )
        assert entropy_gate.passed is False

    def test_healthy_data_passes_all_gates(self):
        """Baseline compared to itself should pass all gates."""
        rows = load_csv(SAMPLE_DIR / "employees_sample.csv")
        profile = compute_entropy_profile(rows, EXCLUDE_COLUMNS)

        detector = DriftDetector(
            entropy_drop_pct=0.50,
            entropy_spike_pct=0.50,
            health_score_floor=0.70,
        )
        health = detector.compute_table_health(profile, profile)

        assert health.health_score == 1.0
        assert health.passed_gate is True
        assert health.columns_drifted == 0

        gate_config = load_gate_config(CONFIG_PATH)
        context = RunContext(
            experiment_id="TEST_INTEGRATION",
            run_id="integration_002",
            git_commit="abc123",
            git_branch="main",
            operator="pytest",
            started_at_utc="2026-03-30T00:00:00Z",
            dry_run=False,
        )
        health_contract = TableHealthResult(
            health_score=health.health_score,
            passed_gate=health.passed_gate,
            total_columns_checked=health.total_columns_checked,
            columns_drifted=health.columns_drifted,
            flagged_columns=health.flagged_columns,
            column_details=health.column_details,
        )
        fidelity = FidelityResult(
            source_row_count=25,
            target_row_count=25,
            row_count_ratio=1.0,
            columns_match=True,
        )
        provenance = build_provenance_envelope(
            context=context,
            status=RunStatus.EXECUTION_COMPLETED,
            completed_at_utc="2026-03-30T00:01:00Z",
            catalog_name="test",
            entropy_health=health_contract,
            tables_processed=("employees",),
        )
        gate_result = evaluate_gates(
            gate_config=gate_config,
            context=context,
            entropy_health=health_contract,
            fidelity=fidelity,
            provenance=provenance,
            quality_pass_ratio=1.0,
            silver_quarantine_ratio=0.0,
        )

        assert gate_result.overall_verdict == "PASS"

    def test_provenance_envelope_complete(self):
        """Provenance should capture all required fields."""
        context = RunContext(
            experiment_id="TEST_INTEGRATION",
            run_id="integration_003",
            git_commit="abc123",
            git_branch="main",
            operator="pytest",
            started_at_utc="2026-03-30T00:00:00Z",
            dry_run=False,
        )
        health = TableHealthResult(
            health_score=0.85,
            passed_gate=True,
            total_columns_checked=5,
            columns_drifted=1,
            flagged_columns=(),
            column_details=(),
        )
        provenance = build_provenance_envelope(
            context=context,
            status=RunStatus.EXECUTION_COMPLETED,
            completed_at_utc="2026-03-30T00:01:00Z",
            catalog_name="test",
            entropy_health=health,
            tables_processed=("employees",),
        )

        assert provenance.experiment_id == "TEST_INTEGRATION"
        assert provenance.run_id == "integration_003"
        assert provenance.git_commit == "abc123"
        assert provenance.git_branch == "main"
        assert provenance.catalog_name == "test"
        assert provenance.operator == "pytest"
        assert provenance.started_at_utc is not None
        assert provenance.completed_at_utc is not None
        assert provenance.entropy_health_score == 0.85
        assert provenance.columns_drifted == 1


class TestRunDemoFunction:
    """Test the run_demo() convenience function."""

    def test_run_demo_returns_structured_results(self):
        results = run_demo()
        assert "context" in results
        assert "baseline_profile" in results
        assert "current_profile" in results
        assert "health" in results
        assert "fidelity" in results
        assert "gate_result" in results
        assert "provenance" in results

    def test_run_demo_verdict_is_fail(self):
        """Default sample data should produce a FAIL verdict."""
        results = run_demo()
        assert results["gate_result"].overall_verdict == "FAIL"

    def test_run_demo_health_below_threshold(self):
        results = run_demo()
        assert results["health"].health_score < 0.70
