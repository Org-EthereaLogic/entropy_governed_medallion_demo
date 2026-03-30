"""
Entropy capture seam for Silver-to-Gold quality governance.

This is the novel seam that differentiates this pipeline from
standard medallion implementations. It uses Shannon Entropy as
the governing quality signal — measuring the information content
of data distributions at the Silver layer and gating Gold refresh
on distribution stability.

This seam replaces proprietary quality formulas with a public,
information-theoretic approach that anyone can verify, reproduce,
and extend.

Author: Anthony Johnson | EthereaLogic LLC
"""

from __future__ import annotations

from typing import Dict, List, Optional

from entropy_governed_medallion.contracts import TableHealthResult
from entropy_governed_medallion.entropy.drift_detector import (
    DriftDetector,
)
from entropy_governed_medallion.entropy.drift_detector import (
    TableHealthResult as DriftTableHealthResult,
)
from entropy_governed_medallion.entropy.shannon import table_entropy_profile


class EntropyCaptureSeam:
    """
    Capture entropy measurements and evaluate distribution health
    as a quality gate between Silver and Gold.

    Usage pattern:
        1. At first Silver load: capture_baseline()
        2. At each subsequent load: measure_and_evaluate()
        3. Runner checks health_result.passed_gate before Gold refresh
    """

    def __init__(
        self,
        entropy_drop_pct: float = 0.50,
        entropy_spike_pct: float = 0.50,
        health_score_floor: float = 0.70,
    ):
        self.detector = DriftDetector(
            entropy_drop_pct=entropy_drop_pct,
            entropy_spike_pct=entropy_spike_pct,
            health_score_floor=health_score_floor,
        )
        self.exclude_columns = [
            "source_system", "source_file_path", "ingest_ts",
        ]

    def compute_profile(
        self, df, exclude_columns: Optional[List[str]] = None,
    ) -> List[Dict]:
        """Compute entropy profile for a DataFrame."""
        exclude = exclude_columns or self.exclude_columns
        return table_entropy_profile(df, exclude_columns=exclude)

    def measure_and_evaluate(
        self,
        baseline_profile: List[Dict],
        current_profile: List[Dict],
        column_weights: Optional[Dict[str, float]] = None,
    ) -> TableHealthResult:
        """
        Compare current entropy against baseline and produce
        a health score that gates Gold refresh.

        This is the core governance decision: if the entropy
        distribution of Silver data has shifted beyond threshold,
        Gold does not refresh — protecting executive KPIs from
        corrupted or drifted source data.
        """
        drift_result: DriftTableHealthResult = self.detector.compute_table_health(
            baseline_profile=baseline_profile,
            current_profile=current_profile,
            column_weights=column_weights,
        )

        # Convert drift detector result to contract type
        from entropy_governed_medallion.contracts import ColumnDriftResult as ContractDrift

        flagged = tuple(
            ContractDrift(
                column_name=f.column_name,
                baseline_entropy=f.baseline_entropy,
                current_entropy=f.current_entropy,
                drift_detected=f.drift_detected,
                drift_direction=f.drift_direction,
                drift_magnitude=f.drift_magnitude,
                stability_score=f.stability_score,
            )
            for f in drift_result.flagged_columns
        )

        details = tuple(
            ContractDrift(
                column_name=d.column_name,
                baseline_entropy=d.baseline_entropy,
                current_entropy=d.current_entropy,
                drift_detected=d.drift_detected,
                drift_direction=d.drift_direction,
                drift_magnitude=d.drift_magnitude,
                stability_score=d.stability_score,
            )
            for d in drift_result.column_details
        )

        return TableHealthResult(
            health_score=drift_result.health_score,
            passed_gate=drift_result.passed_gate,
            total_columns_checked=drift_result.total_columns_checked,
            columns_drifted=drift_result.columns_drifted,
            flagged_columns=flagged,
            column_details=details,
        )
