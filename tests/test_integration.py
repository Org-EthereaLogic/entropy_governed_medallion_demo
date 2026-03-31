"""
End-to-end integration tests for the entropy-governed medallion pipeline.

Loads sample CSVs, computes entropy, detects drift, evaluates gates,
and verifies the full pipeline produces correct structured results.

Author: Anthony Johnson | EthereaLogic LLC
"""

import importlib.util
import json
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path

import entropy_governed_medallion.runners.local_demo as local_demo
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


def _load_visuals_module():
    spec = importlib.util.spec_from_file_location(
        "generate_visuals",
        PROJECT_ROOT / "docs" / "generate_visuals.py",
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


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

    def test_run_demo_uses_entropy_thresholds_from_config(self, tmp_path, monkeypatch):
        custom_config = json.loads(CONFIG_PATH.read_text())
        custom_config["entropy_thresholds"]["health_score_floor"] = 0.10

        custom_path = tmp_path / "kpi_thresholds.json"
        custom_path.write_text(json.dumps(custom_config))

        @contextmanager
        def custom_gate_config_path():
            yield custom_path

        monkeypatch.setattr(local_demo, "_gate_config_path", custom_gate_config_path)

        results = run_demo()

        assert results["health"].health_score == 0.2
        assert results["health"].passed_gate is True


class TestBundledResources:
    """Bundled resources must be usable outside a repo checkout."""

    def test_bundled_sample_csv_available_without_repo_root(self, monkeypatch):
        monkeypatch.setattr(local_demo, "REPO_ROOT", None)
        with local_demo._sample_csv_path("employees_sample.csv") as path:
            rows = load_csv(path)
        assert len(rows) == 25

    def test_bundled_gate_config_available_without_repo_root(self, monkeypatch):
        monkeypatch.setattr(local_demo, "REPO_ROOT", None)
        with local_demo._gate_config_path() as path:
            config = load_gate_config(path)
        assert any(g.metric == "entropy_health_score" for g in config.gates)


class TestRunnerExecution:
    """The documented runner commands should execute without warnings."""

    def test_local_demo_module_runs_without_runtime_warning(self):
        result = subprocess.run(
            [
                sys.executable,
                "-W",
                "error",
                "-m",
                "entropy_governed_medallion.runners.local_demo",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr


class TestDocumentationVisuals:
    """README visuals should reflect the same measured demo outputs."""

    def test_visual_metrics_match_local_demo_measurements(self):
        visuals = _load_visuals_module()
        metrics = visuals.build_visual_metrics()
        results = run_demo()

        monitored_columns = [profile["column_name"] for profile in results["baseline_profile"]]
        assert metrics["monitored_columns"] == monitored_columns
        assert metrics["baseline_health_score"] == 1.0
        assert metrics["current_health_score"] == results["health"].health_score

        gate_metrics = {gate["metric"]: gate for gate in metrics["gates"]}
        gate_evaluations = {
            evaluation.metric: evaluation for evaluation in results["gate_result"].evaluations
        }

        assert gate_metrics["entropy_health_score"]["measured"] == gate_evaluations[
            "entropy_health_score"
        ].measured_value
        assert gate_metrics["entropy_columns_drifted_ratio"]["measured"] == gate_evaluations[
            "entropy_columns_drifted_ratio"
        ].measured_value


class TestTypedConfigContract:
    """Typed config contracts must round-trip through the JSON loader."""

    def test_loaded_config_has_typed_entropy_thresholds(self):
        from entropy_governed_medallion.contracts import EntropyThresholds

        config = load_gate_config(CONFIG_PATH)
        assert isinstance(config.entropy_thresholds, EntropyThresholds)
        assert config.entropy_thresholds.collapse_pct == 0.50
        assert config.entropy_thresholds.spike_pct == 0.50
        assert config.entropy_thresholds.health_score_floor == 0.70
        assert config.entropy_thresholds.baseline_staleness_days == 30

    def test_loaded_config_has_typed_guardrails(self):
        from entropy_governed_medallion.contracts import Guardrails

        config = load_gate_config(CONFIG_PATH)
        assert isinstance(config.guardrails, Guardrails)
        assert config.guardrails.execution_mode == "demo_workspace_only"
        assert config.guardrails.require_unity_catalog is True
        assert "source_system" in config.guardrails.drift_detection_columns_excluded

    def test_loaded_config_has_typed_decision_rule(self):
        from entropy_governed_medallion.contracts import DecisionRule

        config = load_gate_config(CONFIG_PATH)
        assert isinstance(config.decision_rule, DecisionRule)
        assert config.decision_rule.pass_rule != ""
        assert config.decision_rule.gold_blocked != ""

    def test_minimal_json_uses_typed_defaults(self, tmp_path):
        """A JSON with only gates should produce default typed sub-contracts."""
        from entropy_governed_medallion.contracts import (
            DecisionRule,
            EntropyThresholds,
            Guardrails,
        )

        minimal = {"gates": []}
        path = tmp_path / "minimal.json"
        path.write_text(json.dumps(minimal))
        config = load_gate_config(path)
        assert isinstance(config.entropy_thresholds, EntropyThresholds)
        assert config.entropy_thresholds.collapse_pct == 0.50
        assert isinstance(config.guardrails, Guardrails)
        assert isinstance(config.decision_rule, DecisionRule)
