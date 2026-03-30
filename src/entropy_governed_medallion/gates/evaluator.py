"""
Gate evaluator for the entropy-governed medallion pipeline.

Reads evidence from the runner and evaluates each frozen gate metric.
entropy_health_score is a first-class FAIL gate, meaning Gold tables only
refresh when the entropy distribution of Silver data remains stable
relative to baseline.

Null or unmeasured metrics are explicitly marked NOT_MEASURED, never
silently passed. Overall verdict is INCOMPLETE if any gate is unmeasured.

Author: Anthony Johnson | EthereaLogic LLC
"""

from __future__ import annotations

import operator as op_module
from typing import Optional

from entropy_governed_medallion.contracts import (
    EntropyGateConfig,
    FidelityResult,
    GateDefinition,
    GateEvaluation,
    GateEvaluationResult,
    ProvenanceEnvelope,
    RunContext,
    TableHealthResult,
)

OPS = {
    ">=": op_module.ge,
    "<=": op_module.le,
    ">": op_module.gt,
    "<": op_module.lt,
    "==": op_module.eq,
}


def evaluate_gates(
    *,
    gate_config: EntropyGateConfig,
    context: RunContext,
    fidelity: Optional[FidelityResult] = None,
    entropy_health: Optional[TableHealthResult] = None,
    provenance: Optional[ProvenanceEnvelope] = None,
    quality_pass_ratio: Optional[float] = None,
    silver_quarantine_ratio: Optional[float] = None,
) -> GateEvaluationResult:
    """Evaluate all frozen gates and return a structured result."""

    evaluations: list[GateEvaluation] = []
    unmeasured: list[str] = []

    for i, gate in enumerate(gate_config.gates):
        gate_id = f"{gate.type}-{i + 1}"
        measured = _measure_metric(
            gate=gate,
            fidelity=fidelity,
            entropy_health=entropy_health,
            provenance=provenance,
            quality_pass_ratio=quality_pass_ratio,
            silver_quarantine_ratio=silver_quarantine_ratio,
        )

        if measured is None:
            unmeasured.append(gate_id)
            evaluations.append(GateEvaluation(
                gate_id=gate_id,
                metric=gate.metric,
                measured_value=None,
                threshold=gate.threshold,
                op=gate.op,
                gate_type=gate.type,
                passed=None,
                details="NOT_MEASURED",
            ))
        else:
            comparator = OPS.get(gate.op)
            passed = comparator(measured, gate.threshold) if comparator else None
            evaluations.append(GateEvaluation(
                gate_id=gate_id,
                metric=gate.metric,
                measured_value=measured,
                threshold=gate.threshold,
                op=gate.op,
                gate_type=gate.type,
                passed=passed,
                details=f"{measured} {gate.op} {gate.threshold}",
            ))

    verdict = _compute_verdict(evaluations, unmeasured)

    return GateEvaluationResult(
        run_id=context.run_id,
        evaluations=tuple(evaluations),
        overall_verdict=verdict,
        unmeasured_gates=tuple(unmeasured),
    )


def _measure_metric(
    *,
    gate: GateDefinition,
    fidelity: Optional[FidelityResult],
    entropy_health: Optional[TableHealthResult],
    provenance: Optional[ProvenanceEnvelope],
    quality_pass_ratio: Optional[float],
    silver_quarantine_ratio: Optional[float],
) -> Optional[float]:
    """Route each metric name to its measured value."""

    metric = gate.metric

    # --- THE KEY ENTROPY GATE ---
    if metric == "entropy_health_score":
        if entropy_health is None:
            return None
        return entropy_health.health_score

    if metric == "entropy_columns_drifted_ratio":
        if entropy_health is None or entropy_health.total_columns_checked == 0:
            return None
        return entropy_health.columns_drifted / entropy_health.total_columns_checked

    # --- Standard medallion gates ---
    if metric == "bronze_record_fidelity_ratio":
        if fidelity is None or fidelity.row_count_ratio is None:
            return None
        return fidelity.row_count_ratio

    if metric == "silver_quality_pass_ratio":
        return quality_pass_ratio

    if metric == "silver_quarantine_ratio":
        return silver_quarantine_ratio

    if metric == "provenance_field_coverage":
        if provenance is None:
            return None
        return _provenance_coverage(provenance)

    return None


def _provenance_coverage(provenance: ProvenanceEnvelope) -> float:
    """Count non-null provenance fields against required minimums."""
    required = [
        "experiment_id", "run_id", "git_commit", "git_branch",
        "catalog_name", "operator", "started_at_utc",
    ]
    present = 0
    prov_dict = {
        "experiment_id": provenance.experiment_id,
        "run_id": provenance.run_id,
        "git_commit": provenance.git_commit,
        "git_branch": provenance.git_branch,
        "catalog_name": provenance.catalog_name,
        "operator": provenance.operator,
        "started_at_utc": provenance.started_at_utc,
    }
    for f in required:
        if prov_dict.get(f):
            present += 1
    return present / len(required) if required else 0.0


def _compute_verdict(
    evaluations: list[GateEvaluation], unmeasured: list[str]
) -> str:
    if unmeasured:
        return "INCOMPLETE"
    fail_gates = [e for e in evaluations if e.gate_type == "FAIL"]
    warn_gates = [e for e in evaluations if e.gate_type == "WARN"]
    if any(e.passed is False for e in fail_gates):
        return "FAIL"
    if any(e.passed is False for e in warn_gates):
        return "WARN"
    return "PASS"
