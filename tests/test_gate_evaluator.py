"""
Tests for the gate evaluator with entropy health as a FAIL gate.

Author: Anthony Johnson | EthereaLogic LLC
"""

from entropy_governed_medallion.contracts import (
    EntropyGateConfig,
    FidelityResult,
    GateDefinition,
    RunContext,
    TableHealthResult,
)
from entropy_governed_medallion.gates.evaluator import evaluate_gates


def _make_context(**overrides):
    defaults = dict(
        experiment_id="TEST",
        run_id="test_001",
        git_commit="abc123",
        git_branch="main",
        operator="test_runner",
        started_at_utc="2026-03-30T00:00:00Z",
        dry_run=False,
    )
    defaults.update(overrides)
    return RunContext(**defaults)


def _make_gate_config(gates):
    return EntropyGateConfig(gates=tuple(
        GateDefinition(**g) for g in gates
    ))


class TestEntropyGate:
    """The entropy_health_score gate is the key innovation."""

    def test_healthy_entropy_passes(self):
        config = _make_gate_config([{
            "metric": "entropy_health_score",
            "type": "FAIL",
            "op": ">=",
            "threshold": 0.70,
            "reason": "Entropy must be stable.",
        }])
        health = TableHealthResult(
            health_score=0.92,
            passed_gate=True,
            total_columns_checked=5,
            columns_drifted=0,
            flagged_columns=(),
            column_details=(),
        )
        result = evaluate_gates(
            gate_config=config,
            context=_make_context(),
            entropy_health=health,
        )
        assert result.overall_verdict == "PASS"
        assert result.evaluations[0].passed is True

    def test_drifted_entropy_fails(self):
        config = _make_gate_config([{
            "metric": "entropy_health_score",
            "type": "FAIL",
            "op": ">=",
            "threshold": 0.70,
            "reason": "Entropy must be stable.",
        }])
        health = TableHealthResult(
            health_score=0.45,
            passed_gate=False,
            total_columns_checked=5,
            columns_drifted=3,
            flagged_columns=(),
            column_details=(),
        )
        result = evaluate_gates(
            gate_config=config,
            context=_make_context(),
            entropy_health=health,
        )
        assert result.overall_verdict == "FAIL"
        assert result.evaluations[0].passed is False

    def test_missing_entropy_is_incomplete(self):
        config = _make_gate_config([{
            "metric": "entropy_health_score",
            "type": "FAIL",
            "op": ">=",
            "threshold": 0.70,
            "reason": "Entropy must be stable.",
        }])
        result = evaluate_gates(
            gate_config=config,
            context=_make_context(),
            entropy_health=None,
        )
        assert result.overall_verdict == "INCOMPLETE"
        assert len(result.unmeasured_gates) == 1


class TestMultipleGates:
    """Test combined entropy + fidelity + quality gates."""

    def test_all_pass(self):
        config = _make_gate_config([
            {"metric": "entropy_health_score", "type": "FAIL",
             "op": ">=", "threshold": 0.70, "reason": ""},
            {"metric": "bronze_record_fidelity_ratio", "type": "FAIL",
             "op": ">=", "threshold": 0.99, "reason": ""},
            {"metric": "silver_quality_pass_ratio", "type": "FAIL",
             "op": ">=", "threshold": 0.95, "reason": ""},
        ])
        health = TableHealthResult(
            health_score=0.88, passed_gate=True,
            total_columns_checked=5, columns_drifted=0,
            flagged_columns=(), column_details=(),
        )
        fidelity = FidelityResult(
            source_row_count=1000, target_row_count=1000,
            row_count_ratio=1.0, columns_match=True,
        )
        result = evaluate_gates(
            gate_config=config,
            context=_make_context(),
            entropy_health=health,
            fidelity=fidelity,
            quality_pass_ratio=0.97,
        )
        assert result.overall_verdict == "PASS"
        assert all(e.passed for e in result.evaluations)

    def test_entropy_fails_overrides_other_passes(self):
        """Even if fidelity and quality pass, entropy failure blocks Gold."""
        config = _make_gate_config([
            {"metric": "entropy_health_score", "type": "FAIL",
             "op": ">=", "threshold": 0.70, "reason": ""},
            {"metric": "bronze_record_fidelity_ratio", "type": "FAIL",
             "op": ">=", "threshold": 0.99, "reason": ""},
        ])
        health = TableHealthResult(
            health_score=0.35, passed_gate=False,
            total_columns_checked=5, columns_drifted=4,
            flagged_columns=(), column_details=(),
        )
        fidelity = FidelityResult(
            source_row_count=1000, target_row_count=1000,
            row_count_ratio=1.0, columns_match=True,
        )
        result = evaluate_gates(
            gate_config=config,
            context=_make_context(),
            entropy_health=health,
            fidelity=fidelity,
        )
        assert result.overall_verdict == "FAIL"

    def test_warn_gate_produces_warn_verdict(self):
        config = _make_gate_config([
            {"metric": "entropy_health_score", "type": "FAIL",
             "op": ">=", "threshold": 0.70, "reason": ""},
            {"metric": "entropy_columns_drifted_ratio", "type": "WARN",
             "op": "<=", "threshold": 0.20, "reason": ""},
        ])
        health = TableHealthResult(
            health_score=0.75, passed_gate=True,
            total_columns_checked=10, columns_drifted=3,
            flagged_columns=(), column_details=(),
        )
        result = evaluate_gates(
            gate_config=config,
            context=_make_context(),
            entropy_health=health,
        )
        # FAIL gate passes (0.75 >= 0.70), but WARN breaches (0.30 > 0.20)
        assert result.overall_verdict == "WARN"
