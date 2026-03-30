"""
Fidelity capture seam.

Compares source and target row counts after materialization
to produce deterministic fidelity evidence.

Author: Anthony Johnson | EthereaLogic LLC
"""

from __future__ import annotations

from entropy_governed_medallion.contracts import FidelityResult


class FidelityCaptureSeam:
    """Capture source-vs-target fidelity evidence after materialization."""

    def capture(
        self,
        *,
        source_row_count: int,
        target_row_count: int,
        source_columns: list[str],
        target_columns: list[str],
    ) -> FidelityResult:
        if source_row_count > 0:
            ratio = target_row_count / source_row_count
        else:
            ratio = None

        # Ignore CDF metadata columns added by Delta
        cdf_meta = {"_change_type", "_commit_version", "_commit_timestamp"}
        target_clean = set(target_columns) - cdf_meta
        source_set = set(source_columns)

        missing = source_set - target_clean
        extra = target_clean - source_set
        mismatched = []
        for col in sorted(missing):
            mismatched.append(f"missing_in_target:{col}")
        for col in sorted(extra):
            mismatched.append(f"extra_in_target:{col}")

        return FidelityResult(
            source_row_count=source_row_count,
            target_row_count=target_row_count,
            row_count_ratio=ratio,
            columns_match=len(mismatched) == 0,
            mismatched_columns=tuple(mismatched),
        )
