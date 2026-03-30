"""Configuration loading for the entropy-governed pipeline."""

from __future__ import annotations

import json
from pathlib import Path

from entropy_governed_medallion.contracts import (
    EntropyGateConfig,
    GateDefinition,
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

    return EntropyGateConfig(
        gates=gates,
        guardrails=raw.get("guardrails", {}),
        decision_rule=raw.get("decision_rule", {}),
    )
