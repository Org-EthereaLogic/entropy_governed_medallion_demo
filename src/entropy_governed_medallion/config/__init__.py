"""Configuration loading for the entropy-governed pipeline."""

from __future__ import annotations

import json
from pathlib import Path

from entropy_governed_medallion.contracts import (
    DecisionRule,
    EntropyGateConfig,
    EntropyThresholds,
    GateDefinition,
    Guardrails,
)


def load_gate_config(path: Path | str) -> EntropyGateConfig:
    """Load frozen KPI gate thresholds from JSON."""
    with open(path) as f:
        raw = json.load(f)

    gates = tuple(
        GateDefinition(
            metric=g["metric"],
            type=g["type"],
            op=g["op"],
            threshold=g["threshold"],
            reason=g["reason"],
        )
        for g in raw["gates"]
    )

    et_raw = raw.get("entropy_thresholds", {})
    entropy_thresholds = EntropyThresholds(
        collapse_pct=float(et_raw.get("collapse_pct", 0.50)),
        spike_pct=float(et_raw.get("spike_pct", 0.50)),
        health_score_floor=float(et_raw.get("health_score_floor", 0.70)),
        baseline_staleness_days=int(et_raw.get("baseline_staleness_days", 30)),
    )

    gr_raw = raw.get("guardrails", {})
    guardrails = Guardrails(
        execution_mode=str(gr_raw.get("execution_mode", "demo_workspace_only")),
        allowed_catalog_tiers=tuple(gr_raw.get("allowed_catalog_tiers", ("dev",))),
        require_unity_catalog=bool(gr_raw.get("require_unity_catalog", True)),
        entropy_baseline_required_before_gold=bool(
            gr_raw.get("entropy_baseline_required_before_gold", True)
        ),
        drift_detection_columns_excluded=tuple(
            gr_raw.get("drift_detection_columns_excluded", ())
        ),
    )

    dr_raw = raw.get("decision_rule", {})
    decision_rule = DecisionRule(
        pass_rule=str(dr_raw.get("pass", "")),
        warn=str(dr_raw.get("warn", "")),
        fail=str(dr_raw.get("fail", "")),
        gold_blocked=str(dr_raw.get("gold_blocked", "")),
    )

    return EntropyGateConfig(
        gates=gates,
        entropy_thresholds=entropy_thresholds,
        guardrails=guardrails,
        decision_rule=decision_rule,
    )
