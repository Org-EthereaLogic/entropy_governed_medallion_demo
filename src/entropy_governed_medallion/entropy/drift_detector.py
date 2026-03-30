"""
Entropy-based drift detection for Silver-to-Gold quality gates.

Compares current column entropy measurements against stored baselines
to detect distribution changes that rule-based validation would miss.
Entropy stability scoring serves as the governing quality signal.

Author: Anthony Johnson | EthereaLogic LLC
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass(frozen=True)
class ColumnDriftResult:
    """Immutable result of a single column drift evaluation."""
    column_name: str
    baseline_entropy: float
    current_entropy: float
    drift_detected: bool
    drift_direction: str  # STABLE | COLLAPSE | SPIKE
    drift_magnitude: float
    stability_score: float  # 0.0 = maximum drift, 1.0 = no drift


@dataclass(frozen=True)
class TableHealthResult:
    """Immutable composite health result for an entire table."""
    health_score: float  # 0.0–1.0 weighted stability
    passed_gate: bool
    total_columns_checked: int
    columns_drifted: int
    flagged_columns: tuple[ColumnDriftResult, ...]
    column_details: tuple[ColumnDriftResult, ...]


class DriftDetector:
    """
    Detects data distribution drift by comparing entropy measurements
    against stored baselines.

    Configurable thresholds control sensitivity:
    - entropy_drop_pct: flag if entropy drops more than this % from baseline
    - entropy_spike_pct: flag if entropy rises more than this % from baseline
    - health_score_floor: minimum composite score to pass the Gold gate
    """

    def __init__(
        self,
        entropy_drop_pct: float = 0.50,
        entropy_spike_pct: float = 0.50,
        health_score_floor: float = 0.70,
    ):
        self.entropy_drop_pct = entropy_drop_pct
        self.entropy_spike_pct = entropy_spike_pct
        self.health_score_floor = health_score_floor

    def detect_column_drift(
        self,
        column_name: str,
        baseline_entropy: float,
        current_entropy: float,
    ) -> ColumnDriftResult:
        """
        Compare a single column's current entropy against its baseline.
        """
        if baseline_entropy == 0.0 and current_entropy == 0.0:
            return ColumnDriftResult(
                column_name=column_name,
                baseline_entropy=baseline_entropy,
                current_entropy=current_entropy,
                drift_detected=False,
                drift_direction="STABLE",
                drift_magnitude=0.0,
                stability_score=1.0,
            )

        if baseline_entropy == 0.0:
            # Column was constant, now has variation — significant change
            return ColumnDriftResult(
                column_name=column_name,
                baseline_entropy=baseline_entropy,
                current_entropy=current_entropy,
                drift_detected=True,
                drift_direction="SPIKE",
                drift_magnitude=1.0,
                stability_score=0.0,
            )

        pct_change = (current_entropy - baseline_entropy) / baseline_entropy

        if pct_change < -self.entropy_drop_pct:
            return ColumnDriftResult(
                column_name=column_name,
                baseline_entropy=baseline_entropy,
                current_entropy=current_entropy,
                drift_detected=True,
                drift_direction="COLLAPSE",
                drift_magnitude=abs(pct_change),
                stability_score=max(0.0, 1.0 - abs(pct_change)),
            )
        elif pct_change > self.entropy_spike_pct:
            return ColumnDriftResult(
                column_name=column_name,
                baseline_entropy=baseline_entropy,
                current_entropy=current_entropy,
                drift_detected=True,
                drift_direction="SPIKE",
                drift_magnitude=abs(pct_change),
                stability_score=max(0.0, 1.0 - abs(pct_change)),
            )
        else:
            return ColumnDriftResult(
                column_name=column_name,
                baseline_entropy=baseline_entropy,
                current_entropy=current_entropy,
                drift_detected=False,
                drift_direction="STABLE",
                drift_magnitude=abs(pct_change),
                stability_score=max(0.0, 1.0 - abs(pct_change)),
            )

    def compute_table_health(
        self,
        baseline_profile: List[Dict],
        current_profile: List[Dict],
        column_weights: Optional[Dict[str, float]] = None,
    ) -> TableHealthResult:
        """
        Compute a composite health score for a table by comparing
        current entropy profile against baseline.

        Unlike a simple fidelity ratio gate, this catches distribution
        problems, not just row count mismatches.
        """
        baseline_map = {p["column_name"]: p for p in baseline_profile}
        current_map = {p["column_name"]: p for p in current_profile}

        common_columns = sorted(set(baseline_map.keys()) & set(current_map.keys()))

        if not common_columns:
            return TableHealthResult(
                health_score=0.0,
                passed_gate=False,
                total_columns_checked=0,
                columns_drifted=0,
                flagged_columns=(),
                column_details=(),
            )

        weights = column_weights or {c: 1.0 for c in common_columns}
        total_weight = sum(weights.get(c, 1.0) for c in common_columns)

        weighted_stability = 0.0
        column_details: list[ColumnDriftResult] = []
        flagged: list[ColumnDriftResult] = []

        for col in common_columns:
            b = baseline_map[col]["entropy"]
            c = current_map[col]["entropy"]
            w = weights.get(col, 1.0)

            drift = self.detect_column_drift(col, b, c)
            weighted_stability += drift.stability_score * w
            column_details.append(drift)

            if drift.drift_detected:
                flagged.append(drift)

        health_score = round(weighted_stability / total_weight, 4) if total_weight > 0 else 0.0

        return TableHealthResult(
            health_score=health_score,
            passed_gate=health_score >= self.health_score_floor,
            total_columns_checked=len(common_columns),
            columns_drifted=len(flagged),
            flagged_columns=tuple(flagged),
            column_details=tuple(column_details),
        )
